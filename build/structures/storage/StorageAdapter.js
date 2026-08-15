/**
 * StorageAdapter
 *
 * Abstract interface for persisting resume state. ResumeManager delegates
 * all disk/database operations to a StorageAdapter instance, so you can swap
 * the storage backend without touching the restore logic.
 *
 * Built-in adapters:
 *   - {@link JsonFileStorage}     single JSON file (default; fine for small bots)
 *   - {@link ShardedJsonStorage}  one file per guild (scales to thousands of players)
 *   - {@link SqliteStorage}       one SQLite database (ACID, indexed, tens of thousands)
 *
 * To implement a custom adapter (Redis, PostgreSQL, MongoDB, S3, …), extend
 * this class (or just match the method signatures) and pass an instance to
 * `Riffy` via `resume.storage`.
 *
 * All methods are async — even if the underlying store is synchronous (e.g.
 * better-sqlite3), wrap the call in `Promise.resolve(...)` so the contract
 * is uniform and ResumeManager never blocks the event loop unexpectedly.
 *
 * @since 1.0.15
 */
class StorageAdapter {
    /**
     * Called once before any other operation. Use it to open connections,
     * create tables/indices, ensure the directory exists, etc.
     * @returns {Promise<void>}
     */
    async init() {
        /* override in subclass */
    }

    /**
     * Load ALL persisted players into a Map<guildId, SerializedPlayer>.
     * Called once on startup (before restoreAll). For very large stores,
     * you may stream/yield — but the return type must resolve to a complete
     * Map since restoreAll iterates every guild.
     *
     * Implementations MUST skip/repair corrupt entries rather than throw,
     * so one bad guild doesn't prevent restoring the rest.
     *
     * @returns {Promise<Map<string, any>>}
     */
    async loadAll() {
        return new Map();
    }

    /**
     * Persist (or update) the state for a single guild. This is the hot path
     * — called on every player event (debounced by ResumeManager per guild).
     * Must be idempotent and atomic for the single guild.
     *
     * @param {string} guildId
     * @param {any} state The serialized player state (already JSON-safe).
     * @returns {Promise<void>}
     */
    async save(guildId, state) {
        /* override in subclass */
    }

    /**
     * Remove the persisted state for a single guild (e.g. on playerDestroy
     * or after a failed restore). No-op if the guild isn't stored.
     *
     * @param {string} guildId
     * @returns {Promise<void>}
     */
    async remove(guildId) {
        /* override in subclass */
    }

    /**
     * Wipe ALL persisted state. Called by ResumeManager.clear() and (optionally)
     * after a successful restore when `clearOnRestore` is enabled.
     * @returns {Promise<void>}
     */
    async clear() {
        /* override in subclass */
    }

    /**
     * Release resources (close DB handles, file watchers, etc.). Called on
     * process exit / ResumeManager shutdown. Best-effort; must not throw.
     * @returns {Promise<void>}
     */
    async close() {
        /* override in subclass */
    }
}

module.exports = { StorageAdapter };
