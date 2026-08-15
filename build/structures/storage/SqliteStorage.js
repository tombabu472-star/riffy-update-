let Database = null;
try {
    // Lazy-load better-sqlite3 only if the user actually picks this adapter.
    // This keeps `better-sqlite3` an optional peer dependency — Riffy itself
    // stays dependency-free for users who only use the JSON adapters.
    Database = require("better-sqlite3");
} catch (_) {
    Database = null;
}

const { StorageAdapter } = require("./StorageAdapter");

/**
 * SqliteStorage
 *
 * Persists all players in a single SQLite database file with an indexed
 * `players` table. Recommended for large bots (tens of thousands of players)
 * where a sharded-directory of JSON files would create filesystem inode
 * pressure.
 *
 * Benefits over JSON-based adapters:
 *   - ACID transactions (no partial writes ever visible)
 *   - O(log n) indexed lookups by guildId
 *   - Single file (easy to back up / sync)
 *   - Concurrent reads, serialized writes (fine for this workload)
 *
 * Requires `better-sqlite3` to be installed in your project:
 *
 *     npm install better-sqlite3
 *
 * If it's not installed, constructing this adapter throws a clear error
 * pointing you to install it.
 *
 * @since 1.0.15
 */
class SqliteStorage extends StorageAdapter {
    /**
     * @param {Object} options
     * @param {string} [options.path] Path to the SQLite database file.
     *   Default `<cwd>/riffy-state.db`. Use `:memory:` for tests.
     * @param {import("../Riffy").Riffy} [options.riffy] Optional, for debug logging.
     */
    constructor(options = {}, riffy = null) {
        super();
        if (!Database) {
            throw new Error(
                "SqliteStorage requires the 'better-sqlite3' package. Install it with: npm install better-sqlite3"
            );
        }
        this.dbPath = (options && options.path) || "riffy-state.db";
        this.riffy = (options && options.riffy) || riffy || null;
        this._db = null;
    }

    _log(msg) {
        if (this.riffy) this.riffy.emit("debug", `[SqliteStorage] ${msg}`);
    }

    async init() {
        this._db = new Database(this.dbPath);
        // WAL mode for better read concurrency.
        this._db.pragma("journal_mode = WAL");
        this._db.exec(`
            CREATE TABLE IF NOT EXISTS players (
                guildId TEXT PRIMARY KEY,
                state   TEXT NOT NULL,
                updatedAt INTEGER NOT NULL
            );
        `);
        this._stmtUpsert = this._db.prepare(
            `INSERT INTO players (guildId, state, updatedAt) VALUES (?, ?, ?)
             ON CONFLICT(guildId) DO UPDATE SET state = excluded.state, updatedAt = excluded.updatedAt;`
        );
        this._stmtDelete = this._db.prepare(`DELETE FROM players WHERE guildId = ?;`);
        this._stmtAll = this._db.prepare(`SELECT guildId, state FROM players;`);
        this._stmtClear = this._db.prepare(`DELETE FROM players;`);
    }

    async loadAll() {
        const map = new Map();
        if (!this._db) return map;
        const rows = this._stmtAll.all();
        for (const row of rows) {
            try {
                const state = JSON.parse(row.state);
                if (state && typeof state === "object") map.set(row.guildId, state);
            } catch (e) {
                this._log(`Skipping corrupt row for guild ${row.guildId}: ${e.message}`);
            }
        }
        return map;
    }

    async save(guildId, state) {
        // better-sqlite3 is synchronous; wrap in Promise.resolve for contract.
        this._stmtUpsert.run(guildId, JSON.stringify(state), Date.now());
    }

    async remove(guildId) {
        this._stmtDelete.run(guildId);
    }

    async clear() {
        this._stmtClear.run();
    }

    async close() {
        try { if (this._db) this._db.close(); } catch (_) { /* ignore */ }
        this._db = null;
    }
}

module.exports = { SqliteStorage };
