import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { NewsArchive } from "../../src/service/archive.js";
import { createHttpServer } from "../../src/http/server.js";
import { page, standardItems, baseFeed } from "../helpers.js";

const T0 = Date.parse("2026-11-16T02:00:00.000Z");

function fixedClock(ms) {
  return () => new Date(ms);
}

function manualClock(ms) {
  const state = { now: ms };
  return {
    clock: () => new Date(state.now),
    advance(deltaMs) { state.now += deltaMs; }
  };
}

function scriptedFetcher(handler) {
  const calls = [];
  return {
    calls,
    async fetchFeed(feed) {
      calls.push(feed.feedId);
      return handler(feed, calls.length);
    }
  };
}

function okPage(feed, html) {
  return { status: 200, html, requestedUrl: feed.baseUrl, finalUrl: feed.baseUrl, attemptCount: 1 };
}

async function makePair(t, { clockA, clockB, fetcherA, fetcherB, leaseTtlMs = 60_000 }) {
  const directory = await mkdtemp(join(tmpdir(), "news-archive-pair-"));
  const filename = join(directory, "archive.sqlite");
  const a = new NewsArchive(filename, {
    clock: clockA, fetcher: fetcherA, ownerId: "process-a", leaseTtlMs, pollIntervalMs: 5
  });
  const b = new NewsArchive(filename, {
    clock: clockB, fetcher: fetcherB, ownerId: "process-b", leaseTtlMs, pollIntervalMs: 5
  });
  t.after(async () => {
    a.close();
    b.close();
    await rm(directory, { recursive: true, force: true });
  });
  a.registerFeed(baseFeed);
  return { a, b, directory, filename };
}

async function until(condition, attempts = 400) {
  for (let count = 0; count < attempts; count += 1) {
    if (condition()) return;
    await delay(5);
  }
  throw new Error("condition was not met in time");
}

test("takes over an expired lease and fences the stale holder's late page", async (t) => {
  let releaseA;
  const gateA = new Promise((resolve) => { releaseA = resolve; });
  const fetcherA = scriptedFetcher(async (feed) => {
    await gateA;
    return okPage(feed, page(standardItems(4)));
  });
  const fetcherB = scriptedFetcher(async (feed) => okPage(feed, page(standardItems(4, 10))));
  const { a, b } = await makePair(t, {
    clockA: fixedClock(T0), clockB: fixedClock(T0 + 10 * 60_000), fetcherA, fetcherB
  });

  // Process A becomes the holder and gets stuck inside its fetch; its lease
  // (valid until T0 + 60s) is long expired on process B's clock.
  const first = a.refreshFeed("daily-main");
  await until(() => fetcherA.calls.length === 1);

  const takeover = await b.refreshFeed("daily-main");
  assert.equal(takeover.outcome, "accepted");
  assert.equal(fetcherB.calls.length, 1);
  assert.equal(b.articles.stats("daily-main").total, 4);
  assert.ok(b.listItems("daily-main").items.every((item) => /article-1[0-3]\.shtml$/.test(item.canonicalUrl)));

  // The stuck holder finally returns with its late page; it must not be applied
  // on top of the list the successor already updated.
  releaseA();
  const stale = await first;
  assert.equal(stale.outcome, "superseded");
  assert.equal(stale.capture, null);
  assert.equal(b.listCaptures("daily-main").length, 1);
  assert.equal(b.articles.stats("daily-main").total, 4);
  assert.ok(b.listItems("daily-main").items.every((item) => /article-1[0-3]\.shtml$/.test(item.canonicalUrl)));

  // Both network requests really happened and stay audited, but only the
  // successor's attempt and its capture claim the success.
  const attempts = b.listFetchAttempts("daily-main");
  assert.equal(attempts.length, 2);
  const byOutcome = Object.fromEntries(attempts.map((attempt) => [attempt.outcome, attempt]));
  assert.equal(byOutcome.accepted.captureId, takeover.capture.captureId);
  assert.equal(byOutcome.superseded.captureId, null);
  assert.equal(byOutcome.superseded.errorCode, "lease_lost");
  assert.equal(byOutcome.superseded.httpStatus, 200);

  // The logical task states are queryable from either connection.
  const current = b.refreshStatus("daily-main").current;
  assert.equal(current.status, "completed");
  assert.equal(current.ownerId, "process-b");
  assert.equal(current.result.attemptId, takeover.attemptId);
  assert.equal(a.runs.get(stale.runId).status, "superseded");
  assert.ok(b.runs.get(stale.runId).fencingToken < current.fencingToken);
});

test("joins a live cross-process run and lets the waiter observe the shared result", async (t) => {
  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  const fetcherA = scriptedFetcher(async (feed) => {
    await gate;
    return okPage(feed, page(standardItems(5)));
  });
  const fetcherB = scriptedFetcher(async () => {
    throw new Error("the waiting process must not issue its own fetch");
  });
  const shared = manualClock(T0);
  const { a, b } = await makePair(t, { clockA: shared.clock, clockB: shared.clock, fetcherA, fetcherB });

  const first = a.refreshFeed("daily-main");
  await until(() => fetcherA.calls.length === 1);
  const second = b.refreshFeed("daily-main");

  // The waiter can inspect the in-progress logical task owned by process A.
  await until(() => b.refreshStatus("daily-main").current?.status === "running");
  const pending = b.refreshStatus("daily-main").current;
  assert.equal(pending.ownerId, "process-a");
  assert.equal(pending.result, null);

  release();
  const [holderResult, waiterResult] = await Promise.all([first, second]);
  assert.equal(waiterResult.attemptId, holderResult.attemptId);
  assert.equal(waiterResult.outcome, "accepted");
  assert.equal(fetcherB.calls.length, 0);
  assert.equal(b.listFetchAttempts("daily-main").length, 1);

  const done = b.refreshStatus("daily-main").current;
  assert.equal(done.status, "completed");
  assert.equal(done.result.attemptId, holderResult.attemptId);
  assert.equal(done.result.capture.accepted, true);
});

test("rolls back an interrupted commit and lets the other process recover", async (t) => {
  const fetcherA = scriptedFetcher(async (feed) => okPage(feed, page(standardItems(4))));
  const fetcherB = scriptedFetcher(async (feed) => okPage(feed, page(standardItems(4, 20))));
  const { a, b } = await makePair(t, {
    clockA: fixedClock(T0), clockB: fixedClock(T0 + 1_000), fetcherA, fetcherB
  });
  a.database.exec(`CREATE TRIGGER fail_capture_link BEFORE INSERT ON capture_items
    BEGIN SELECT RAISE(ABORT, 'simulated storage fault'); END;`);

  await assert.rejects(a.refreshFeed("daily-main"), /simulated storage fault/);

  // The interrupted commit left nothing half-written on the shared state.
  assert.equal(b.listCaptures("daily-main").length, 0);
  assert.equal(b.articles.stats("daily-main").total, 0);
  assert.equal(b.feedStatus("daily-main").lastAcceptedAt, null);

  // The network request that really happened is still audited as a failed refresh.
  const [failed] = b.listFetchAttempts("daily-main");
  assert.equal(failed.outcome, "failed");
  assert.equal(failed.httpStatus, 200);
  assert.equal(failed.errorMessage, "simulated storage fault");
  assert.equal(a.refreshStatus("daily-main").current.status, "failed");

  // The other process recovers with a fresh run once the fault is gone.
  a.database.exec("DROP TRIGGER fail_capture_link");
  const recovered = await b.refreshFeed("daily-main");
  assert.equal(recovered.outcome, "accepted");
  assert.equal(b.articles.stats("daily-main").total, 4);
  assert.ok(b.listItems("daily-main").items.every((item) => /article-2[0-3]\.shtml$/.test(item.canonicalUrl)));
  assert.equal(b.refreshStatus("daily-main").current.status, "completed");
});

test("recovers when the holder process exits before finishing its run", async (t) => {
  let releaseA;
  const gateA = new Promise((resolve) => { releaseA = resolve; });
  const fetcherA = scriptedFetcher(async (feed) => {
    await gateA;
    return okPage(feed, page(standardItems(4)));
  });
  const fetcherB = scriptedFetcher(async (feed) => okPage(feed, page(standardItems(4, 30))));
  const clockB = manualClock(T0);
  const { a, b } = await makePair(t, { clockA: fixedClock(T0), clockB: clockB.clock, fetcherA, fetcherB });

  const first = a.refreshFeed("daily-main");
  const settled = first.then((result) => ({ result }), (error) => ({ error }));
  await until(() => fetcherA.calls.length === 1);

  // The holder process dies mid-flight: its database connection is gone.
  a.close();
  clockB.advance(10 * 60_000);

  const recovered = await b.refreshFeed("daily-main");
  assert.equal(recovered.outcome, "accepted");
  assert.equal(b.articles.stats("daily-main").total, 4);

  // The dead holder's late response cannot disturb the recovered archive.
  releaseA();
  const { error } = await settled;
  assert.ok(error instanceof Error);
  assert.equal(b.listCaptures("daily-main").length, 1);
  assert.equal(b.articles.stats("daily-main").total, 4);
  assert.ok(b.listItems("daily-main").items.every((item) => /article-3[0-3]\.shtml$/.test(item.canonicalUrl)));
  assert.equal(b.listFetchAttempts("daily-main").length, 1);
});

test("serves the coordinated refresh run over HTTP", async (t) => {
  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  const fetcherA = scriptedFetcher(async (feed) => {
    await gate;
    return okPage(feed, page(standardItems(4)));
  });
  const fetcherB = scriptedFetcher(async () => {
    throw new Error("unused connection must not fetch");
  });
  const shared = manualClock(T0);
  const { a, b } = await makePair(t, { clockA: shared.clock, clockB: shared.clock, fetcherA, fetcherB });
  const app = createHttpServer(b, { host: "127.0.0.1", port: 0, logger: { error() {} } });
  const address = await app.listen();
  t.after(() => app.close());
  const root = `http://127.0.0.1:${address.port}/v1/feeds`;

  const first = a.refreshFeed("daily-main");
  await until(() => fetcherA.calls.length === 1);

  const running = await fetch(`${root}/daily-main/refresh`);
  assert.equal(running.status, 200);
  const runningBody = await running.json();
  assert.equal(runningBody.current.status, "running");
  assert.equal(runningBody.current.ownerId, "process-a");
  assert.equal(runningBody.current.result, null);

  release();
  const holderResult = await first;
  const finished = await fetch(`${root}/daily-main/refresh`);
  const finishedBody = await finished.json();
  assert.equal(finishedBody.current.status, "completed");
  assert.equal(finishedBody.current.result.outcome, "accepted");
  assert.equal(finishedBody.current.result.attemptId, holderResult.attemptId);

  const missing = await fetch(`${root}/unknown-feed/refresh`);
  assert.equal(missing.status, 404);
});
