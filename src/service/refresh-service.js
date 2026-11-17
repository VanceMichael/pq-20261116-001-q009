import { setTimeout as delay } from "node:timers/promises";

const DEFAULT_LEASE_TTL_MS = 120_000;
const DEFAULT_POLL_INTERVAL_MS = 50;

/**
 * Coordinates feed refreshes across every process attached to the same database.
 *
 * Each refresh is a logical task stored in refresh_runs. The first process to ask
 * becomes the holder and does the network fetch; concurrent callers (in any process)
 * join the same run and share its stored result. A run's lease can expire while its
 * holder is stuck or gone, in which case the next caller takes over with a higher
 * fencing token. A stale holder that comes back afterwards is fenced at commit time:
 * its late page is never applied to the archive, but the fetch it really performed
 * stays audited as a 'superseded' attempt.
 */
export class RefreshService {
  constructor({ database, feeds, fetches, runs, fetcher, imports, clock = () => new Date(),
    ownerId, leaseTtlMs = DEFAULT_LEASE_TTL_MS, pollIntervalMs = DEFAULT_POLL_INTERVAL_MS, sleep = delay }) {
    if (typeof ownerId !== "string" || ownerId.length === 0) {
      throw new TypeError("a non-empty ownerId is required to coordinate refreshes");
    }
    this.database = database;
    this.feeds = feeds;
    this.fetches = fetches;
    this.runs = runs;
    this.fetcher = fetcher;
    this.imports = imports;
    this.clock = clock;
    this.ownerId = ownerId;
    this.leaseTtlMs = leaseTtlMs;
    this.pollIntervalMs = pollIntervalMs;
    this.sleep = sleep;
    this.inFlight = new Map();
  }

  refresh(feedId) {
    const active = this.inFlight.get(feedId);
    if (active) return active;
    const task = this.perform(feedId).finally(() => {
      if (this.inFlight.get(feedId) === task) this.inFlight.delete(feedId);
    });
    this.inFlight.set(feedId, task);
    return task;
  }

  async perform(feedId) {
    const feed = this.feeds.get(feedId);
    if (!feed) throw new Error(`unknown feed: ${feedId}`);
    for (;;) {
      const acquisition = this.acquire(feed);
      if (acquisition.role === "holder") return this.performAsHolder(feed, acquisition.run);
      const settled = await this.awaitRun(acquisition.run.runId);
      if (settled.status === "completed") return settled.result;
      if (settled.status === "failed") throw runFailure(settled);
      // The run expired or was superseded without a usable result: take over or
      // join the successor run on the next iteration.
    }
  }

  acquire(feed) {
    const now = this.clock();
    const nowIso = now.toISOString();
    const leaseExpiresAt = new Date(now.getTime() + this.leaseTtlMs).toISOString();
    this.database.exec("BEGIN IMMEDIATE");
    try {
      const running = this.runs.running(feed.feedId);
      if (running && running.leaseExpiresAt > nowIso) {
        this.database.exec("COMMIT");
        return { role: "join", run: running };
      }
      if (running) this.runs.expire(running.runId, nowIso);
      const run = this.runs.create({
        feedId: feed.feedId,
        ownerId: this.ownerId,
        fencingToken: this.runs.nextToken(feed.feedId),
        startedAt: nowIso,
        leaseExpiresAt
      });
      this.database.exec("COMMIT");
      return { role: "holder", run };
    } catch (error) {
      if (this.database.isTransaction) this.database.exec("ROLLBACK");
      throw error;
    }
  }

  async awaitRun(runId) {
    for (;;) {
      const run = this.runs.get(runId);
      if (!run) return { status: "expired", result: null };
      if (run.status === "completed" || run.status === "failed") return run;
      if (run.status !== "running") return { status: "expired", result: null };
      if (run.leaseExpiresAt <= this.clock().toISOString()) return { status: "expired", result: null };
      await this.sleep(this.pollIntervalMs);
    }
  }

  async performAsHolder(feed, run) {
    const startedAt = this.clock().toISOString();
    let fetched;
    try {
      fetched = await this.fetcher.fetchFeed(feed);
    } catch (error) {
      const attemptId = this.recordFailure({ feed, run, startedAt, error,
        httpStatus: error.status ?? null, attemptCount: error.attemptCount ?? 1,
        requestedUrl: feed.baseUrl, finalUrl: null });
      throw Object.assign(error, { attemptId, statusCode: error.statusCode ?? 502 });
    }
    const finishedAt = this.clock().toISOString();
    try {
      if (fetched.status === 304) return this.commitNotModified(feed, run, fetched, startedAt, finishedAt);
      return this.commitImport(feed, run, fetched, startedAt, finishedAt);
    } catch (error) {
      // The commit transaction was interrupted and rolled back; keep the network
      // request that really happened audited as a failed attempt in a fresh transaction.
      const attemptId = this.recordFailure({ feed, run, startedAt, error,
        httpStatus: fetched.status ?? null, attemptCount: fetched.attemptCount ?? 1,
        requestedUrl: fetched.requestedUrl ?? feed.baseUrl, finalUrl: fetched.finalUrl ?? null });
      throw Object.assign(error, { attemptId, statusCode: error.statusCode ?? 502 });
    }
  }

  commitImport(feed, run, fetched, startedAt, finishedAt) {
    this.database.exec("BEGIN IMMEDIATE");
    try {
      if (!this.stillHolder(run)) {
        const result = this.recordSuperseded(feed, run, fetched, startedAt, finishedAt);
        this.database.exec("COMMIT");
        return result;
      }
      const capture = this.imports.importHtml(feed.feedId, fetched.html, finishedAt);
      const attemptId = this.fetches.record({
        feedId: feed.feedId, startedAt, finishedAt, outcome: capture.accepted ? "accepted" : "rejected",
        httpStatus: fetched.status, attemptCount: fetched.attemptCount,
        requestedUrl: fetched.requestedUrl, finalUrl: fetched.finalUrl,
        captureId: capture.captureId ?? null, errorCode: capture.accepted ? null : "capture_rejected",
        errorMessage: capture.reason
      });
      const result = { runId: run.runId, attemptId, feedId: feed.feedId,
        outcome: capture.accepted ? "accepted" : "rejected", httpStatus: fetched.status, capture };
      this.runs.finish({ runId: run.runId, status: "completed", result, attemptId, finishedAt });
      this.database.exec("COMMIT");
      return result;
    } catch (error) {
      if (this.database.isTransaction) this.database.exec("ROLLBACK");
      throw error;
    }
  }

  commitNotModified(feed, run, fetched, startedAt, finishedAt) {
    this.database.exec("BEGIN IMMEDIATE");
    try {
      if (!this.stillHolder(run)) {
        const result = this.recordSuperseded(feed, run, fetched, startedAt, finishedAt);
        this.database.exec("COMMIT");
        return result;
      }
      const attemptId = this.fetches.record({
        feedId: feed.feedId, startedAt, finishedAt, outcome: "not_modified", httpStatus: 304,
        attemptCount: fetched.attemptCount, requestedUrl: fetched.requestedUrl,
        finalUrl: fetched.finalUrl, captureId: null, errorCode: null, errorMessage: null
      });
      const result = { runId: run.runId, attemptId, feedId: feed.feedId,
        outcome: "not_modified", httpStatus: 304, capture: null };
      this.runs.finish({ runId: run.runId, status: "completed", result, attemptId, finishedAt });
      this.database.exec("COMMIT");
      return result;
    } catch (error) {
      if (this.database.isTransaction) this.database.exec("ROLLBACK");
      throw error;
    }
  }

  stillHolder(run) {
    const fresh = this.runs.get(run.runId);
    if (!fresh || fresh.status !== "running") return false;
    return fresh.fencingToken === this.runs.currentToken(run.feedId);
  }

  recordSuperseded(feed, run, fetched, startedAt, finishedAt) {
    const attemptId = this.fetches.record({
      feedId: feed.feedId, startedAt, finishedAt, outcome: "superseded",
      httpStatus: fetched.status ?? null, attemptCount: fetched.attemptCount ?? 1,
      requestedUrl: fetched.requestedUrl ?? feed.baseUrl, finalUrl: fetched.finalUrl ?? null,
      captureId: null, errorCode: "lease_lost",
      errorMessage: "refresh lease was taken over by a newer run; the fetched page was not applied"
    });
    const result = { runId: run.runId, attemptId, feedId: feed.feedId, outcome: "superseded",
      httpStatus: fetched.status ?? null, capture: null };
    this.runs.finish({ runId: run.runId, status: "superseded", result, attemptId, finishedAt });
    return result;
  }

  recordFailure({ feed, run, startedAt, error, httpStatus, attemptCount, requestedUrl, finalUrl }) {
    const code = error.code ?? "refresh_failed";
    const message = error.message.slice(0, 500);
    try {
      const finishedAt = this.clock().toISOString();
      this.database.exec("BEGIN IMMEDIATE");
      const attemptId = this.fetches.record({
        feedId: feed.feedId, startedAt, finishedAt, outcome: "failed", httpStatus,
        attemptCount, requestedUrl, finalUrl, captureId: null, errorCode: code, errorMessage: message
      });
      const result = { runId: run.runId, attemptId, feedId: feed.feedId, outcome: "failed",
        error: { code, message, statusCode: error.statusCode ?? 502,
          status: error.status ?? null, attemptCount: error.attemptCount ?? attemptCount } };
      this.runs.finish({ runId: run.runId, status: "failed", result, attemptId, finishedAt });
      this.database.exec("COMMIT");
      return attemptId;
    } catch {
      // The database may already be gone (process exiting); the original error still propagates.
      if (this.database.isOpen && this.database.isTransaction) this.database.exec("ROLLBACK");
      return undefined;
    }
  }

  recent(feedId, limit) {
    if (!this.feeds.get(feedId)) return null;
    return this.fetches.recent(feedId, limit);
  }

  current(feedId) {
    return this.runs.latest(feedId);
  }

  pending(feedId) {
    return this.inFlight.has(feedId);
  }
}

function runFailure(run) {
  const failure = run.result?.error ?? { code: "refresh_failed", message: "refresh run failed", statusCode: 502 };
  const error = Object.assign(new Error(failure.message), {
    code: failure.code,
    attemptId: run.result?.attemptId ?? run.attemptId ?? undefined,
    statusCode: failure.statusCode ?? 502
  });
  if (failure.status != null) error.status = failure.status;
  if (failure.attemptCount != null) error.attemptCount = failure.attemptCount;
  return error;
}
