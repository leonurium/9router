import Database from "better-sqlite3";
import { PRAGMA_SQL } from "../schema.js";
import { DATA_FILE } from "../paths.js";
import { fetchFromGist, uploadToGist, createGist } from "./gistSync.js";

// Periodic checkpoint to keep WAL file small (avoid huge -wal/-shm growth)
const CHECKPOINT_INTERVAL_MS = 60 * 1000;
const BACKUP_INTERVAL_MS = 60 * 60 * 1000; // backup to Gist every 1 hour

const githubToken = process.env.GITHUB_TOKEN;
const initialGistId = process.env.GITHUB_GIST_ID || null;
const hasGithub = !!githubToken;

// Track gist ID (may be created on first backup)
let gistId = initialGistId;

// Top-level await: block until gist restore is done before opening DB
async function gistRestore(filePath) {
  if (!hasGithub) return false;
  try {
    console.log("[gist] attempting restore...");
    const buf = await fetchFromGist(gistId, githubToken);
    if (!buf) {
      console.log("[gist] no backup found — starting fresh");
      return false;
    }
    // Validate SQLite magic header before writing — prevents corrupted backups from
    // creating garbage DB files. SQLite files start with "SQLite format 3\0" (16 bytes).
    const SQLITE_MAGIC = "SQLite format 3\0";
    const header = buf.slice(0, 16).toString("utf8");
    if (header !== SQLITE_MAGIC) {
      console.warn(`[gist] restore: invalid SQLite header (got: ${JSON.stringify(header)}), discarding and starting fresh`);
      return false;
    }
    const fs = await import("node:fs");
    fs.writeFileSync(filePath, buf);
    console.log(`[gist] restored ${buf.length} bytes from backup (validated SQLite header)`);
    return true;
  } catch (e) {
    console.warn(`[gist] restore failed (will use local): ${e.message}`);
    return false;
  }
}

async function gistBackup(filePath) {
  if (!hasGithub) return;
  try {
    const fs = await import("node:fs");
    const buf = fs.readFileSync(filePath);
    const newGistId = await uploadToGist(gistId, githubToken, buf);
    if (!gistId && newGistId) {
      gistId = newGistId;
      console.log(`[gist] backup: created new gist: ${gistId}`);
    } else {
      console.log(`[gist] backed up ${buf.length} bytes`);
    }
  } catch (e) {
    console.error(`[gist] backup failed: ${e.message}`);
  }
}

console.log(`[gist] restore: starting (gistId: ${gistId || "none"}, token: ${hasGithub ? "yes" : "no"})`);
await gistRestore(DATA_FILE);

export function createBetterSqliteAdapter(filePath) {
  const db = new Database(filePath);
  db.exec(PRAGMA_SQL);
  // Schema is created/synced by migrate.js after adapter init

  const stmtCache = new Map();

  function prepare(sql) {
    let stmt = stmtCache.get(sql);
    if (!stmt) {
      stmt = db.prepare(sql);
      stmtCache.set(sql, stmt);
    }
    return stmt;
  }

  // Truncate WAL periodically so file stays small for backup/copy
  const checkpointTimer = setInterval(() => {
    try { db.pragma("wal_checkpoint(TRUNCATE)"); } catch {}
  }, CHECKPOINT_INTERVAL_MS);
  if (typeof checkpointTimer.unref === "function") checkpointTimer.unref();

  let backupTimer = null;
  if (hasGithub) {
    console.log(`[better-sqlite3] gist periodic backup: enabled (interval: ${BACKUP_INTERVAL_MS / 1000}s)`);
    backupTimer = setInterval(() => {
      try { db.pragma("wal_checkpoint(TRUNCATE)"); } catch {}
      gistBackup(filePath).catch((e) => console.error(`[better-sqlite3] periodic backup error: ${e.message}`));
    }, BACKUP_INTERVAL_MS);
  } else {
    console.log("[better-sqlite3] gist periodic backup: disabled (no GITHUB_TOKEN env var)");
  }

  function gracefulClose() {
    try { db.pragma("wal_checkpoint(TRUNCATE)"); } catch {}
    try { stmtCache.clear(); } catch {}
    try { db.close(); } catch {}
  }

  // Ensure WAL is flushed and -wal/-shm files removed on shutdown
  const onShutdown = async () => {
    console.log("[better-sqlite3] shutdown: flushing WAL...");
    try { db.pragma("wal_checkpoint(TRUNCATE)"); } catch {}
    try { stmtCache.clear(); } catch {}
    try { db.close(); } catch {}
    if (hasGithub) {
      console.log("[better-sqlite3] shutdown: final gist backup...");
      await gistBackup(filePath);
      console.log("[better-sqlite3] shutdown: done");
    }
  };
  process.once("beforeExit", () => onShutdown());
  process.once("SIGINT", () => { onShutdown(); process.exit(0); });
  process.once("SIGTERM", async () => { await onShutdown(); process.exit(0); });

  return {
    driver: "better-sqlite3",
    run(sql, params = []) { return prepare(sql).run(params); },
    get(sql, params = []) { return prepare(sql).get(params); },
    all(sql, params = []) { return prepare(sql).all(params); },
    exec(sql) { return db.exec(sql); },
    transaction(fn) { return db.transaction(fn)(); },
    checkpoint() { try { db.pragma("wal_checkpoint(TRUNCATE)"); } catch {} },
    close() {
      clearInterval(checkpointTimer);
      if (backupTimer) clearInterval(backupTimer);
      gracefulClose();
    },
    raw: db,
  };
}
