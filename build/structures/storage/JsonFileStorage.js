const fs = require("node:fs");
const path = require("node:path");
const { StorageAdapter } = require("./StorageAdapter");

/**
 * JsonFileStorage
 *
 * Persists all players in a single JSON file. This is the default adapter and
 * the original behavior of ResumeManager — fine for small/medium bots (up to
 * ~100 concurrent players).
 *
 * For larger bots, use {@link ShardedJsonStorage} or {@link SqliteStorage}
 * instead: a single file suffers from read/write amplification (every save
 * rewrites every guild's state) and a crash mid-write can corrupt all guilds.
 *
 * @since 1.0.15
 */
class JsonFileStorage extends StorageAdapter {
    /**
     * @param {Object} [options]
     * @param {string} [options.filePath] Path to the JSON file. Default `<cwd>/riffy-state.json`.
     * @param {import("../Riffy").Riffy} [riffy] Optional, for debug logging.
     */
    constructor(options = {}, riffy = null) {
        super();
        this.filePath = options.filePath || path.join(process.cwd(), "riffy-state.json");
        this.riffy = riffy;
    }

    _log(msg) {
        if (this.riffy) this.riffy.emit("debug", `[JsonFileStorage] ${msg}`);
    }

    async init() {
        const dir = path.dirname(this.filePath);
        if (dir && !fs.existsSync(dir)) {
            try { fs.mkdirSync(dir, { recursive: true }); } catch (_) { /* ignore */ }
        }
    }

    async loadAll() {
        const map = new Map();
        try {
            if (!fs.existsSync(this.filePath)) return map;
            const raw = fs.readFileSync(this.filePath, "utf-8");
            const parsed = JSON.parse(raw);
            if (!parsed || typeof parsed !== "object" || !parsed.players) return map;
            for (const [guildId, state] of Object.entries(parsed.players)) {
                if (state && typeof state === "object") {
                    map.set(guildId, state);
                }
            }
        } catch (e) {
            this._log(`Failed to load/parse ${this.filePath}: ${e.message}. Moving it aside.`);
            // Move the corrupt file aside so future saves aren't blocked by it.
            try {
                if (fs.existsSync(this.filePath)) {
                    const backup = this.filePath + ".corrupt-" + Date.now();
                    fs.renameSync(this.filePath, backup);
                    this._log(`Moved corrupt file to ${backup}`);
                }
            } catch (_) { /* ignore */ }
        }
        return map;
    }

    async save(guildId, state) {
        this.saveSync(guildId, state);
    }

    /**
     * Synchronous version of save() — used on process.exit where async I/O
     * is unreliable (the event loop is torn down). Writes to a temp file
     * then renames atomically.
     * @since 1.0.15
     */
    saveSync(guildId, state) {
        // Read current file, merge the single guild, write back.
        // (Single-file adapter — inherent write amplification vs. sharded.)
        let parsed = { version: 1, savedAt: 0, players: {} };
        try {
            if (fs.existsSync(this.filePath)) {
                parsed = JSON.parse(fs.readFileSync(this.filePath, "utf-8")) || parsed;
                if (!parsed.players) parsed.players = {};
            }
        } catch (e) {
            // File is corrupt — start fresh rather than lose this guild's save.
            this._log(`Existing file corrupt, starting fresh: ${e.message}`);
            parsed = { version: 1, savedAt: 0, players: {} };
        }
        parsed.savedAt = Date.now();
        parsed.players[guildId] = state;
        // Atomic write: temp file + rename.
        const tmp = this.filePath + ".tmp-" + process.pid;
        fs.writeFileSync(tmp, JSON.stringify(parsed, null, 2), "utf-8");
        fs.renameSync(tmp, this.filePath);
    }

    async remove(guildId) {
        try {
            if (!fs.existsSync(this.filePath)) return;
            const parsed = JSON.parse(fs.readFileSync(this.filePath, "utf-8"));
            if (!parsed?.players || !(guildId in parsed.players)) return;
            delete parsed.players[guildId];
            parsed.savedAt = Date.now();
            const tmp = this.filePath + ".tmp-" + process.pid;
            fs.writeFileSync(tmp, JSON.stringify(parsed, null, 2), "utf-8");
            fs.renameSync(tmp, this.filePath);
        } catch (e) {
            this._log(`remove() failed for ${guildId}: ${e.message}`);
        }
    }

    async clear() {
        try {
            if (fs.existsSync(this.filePath)) fs.unlinkSync(this.filePath);
        } catch (e) {
            this._log(`clear() failed: ${e.message}`);
        }
    }

    async close() { /* no resources to release */ }
}

module.exports = { JsonFileStorage };
