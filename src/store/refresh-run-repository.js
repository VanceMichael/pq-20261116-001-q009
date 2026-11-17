export class RefreshRunRepository {
  constructor(database) {
    this.database = database;
    this.insertRun = database.prepare(`INSERT INTO refresh_runs
      (feed_id, owner_id, fencing_token, status, started_at, lease_expires_at)
      VALUES (?, ?, ?, 'running', ?, ?)`);
    this.getRun = database.prepare("SELECT * FROM refresh_runs WHERE run_id=?");
    this.latestRunning = database.prepare(`SELECT * FROM refresh_runs
      WHERE feed_id=? AND status='running' ORDER BY run_id DESC LIMIT 1`);
    this.latestRun = database.prepare(`SELECT * FROM refresh_runs
      WHERE feed_id=? ORDER BY run_id DESC LIMIT 1`);
    this.maxToken = database.prepare(`SELECT COALESCE(MAX(fencing_token), 0) AS token
      FROM refresh_runs WHERE feed_id=?`);
    this.finishRun = database.prepare(`UPDATE refresh_runs
      SET status=?, finished_at=?, result_json=?, attempt_id=? WHERE run_id=?`);
    this.expireRun = database.prepare(`UPDATE refresh_runs
      SET status='expired', finished_at=? WHERE run_id=? AND status='running'`);
  }

  create({ feedId, ownerId, fencingToken, startedAt, leaseExpiresAt }) {
    const result = this.insertRun.run(feedId, ownerId, fencingToken, startedAt, leaseExpiresAt);
    return this.get(Number(result.lastInsertRowid));
  }

  get(runId) {
    const row = this.getRun.get(runId);
    return row ? mapRun(row) : null;
  }

  running(feedId) {
    const row = this.latestRunning.get(feedId);
    return row ? mapRun(row) : null;
  }

  latest(feedId) {
    const row = this.latestRun.get(feedId);
    return row ? mapRun(row) : null;
  }

  currentToken(feedId) {
    return this.maxToken.get(feedId).token;
  }

  nextToken(feedId) {
    return this.currentToken(feedId) + 1;
  }

  finish({ runId, status, result, attemptId = null, finishedAt }) {
    this.finishRun.run(status, finishedAt, result === undefined ? null : JSON.stringify(result), attemptId, runId);
  }

  expire(runId, finishedAt) {
    this.expireRun.run(finishedAt, runId);
  }
}

function mapRun(row) {
  return {
    runId: row.run_id,
    feedId: row.feed_id,
    ownerId: row.owner_id,
    fencingToken: row.fencing_token,
    status: row.status,
    startedAt: row.started_at,
    leaseExpiresAt: row.lease_expires_at,
    finishedAt: row.finished_at,
    attemptId: row.attempt_id,
    result: row.result_json ? JSON.parse(row.result_json) : null
  };
}
