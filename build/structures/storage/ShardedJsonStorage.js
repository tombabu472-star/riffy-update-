const fs = require("node:fs");
const path = require("node:path");
const { StorageAdapter } = require("./StorageAdapter");

/**
 * ShardedJsonStorage
 *
 * Persists each guild's state in its own JSON file inside a directory:
 *
 *     <dir>/<guildId>.json
 *
 * This is the recommended adapter for medium-to-large bots (hundreds to
 * thousands of concurrent players). It solves the three problems a single
 * JSON file has at scale:
 *
 *   1. **No write amplification** — saving guild A doesn't touch guild B's
 *      file. Each playerUpdate only rewrites that one guild's ~1-5 KB file.
 *   2. **No read amplification** — loadAll() reads each file independently;
 *      a corrupt file only loses that one guild, not everyone.
 *   3. **Crash isolation** — a mid-write crash (power loss, SIGKILL) can
 *      corrupt at most one guild's file, not the whole state.
 *
 * For tens of thousands of players, consider {@link SqliteStorage} (one DB
 * file, indexed, ACID) to avoid filesystem inode pressure.
 *
 * @since 1.0.15
 */
class ShardedJsonStorage extends StorageAdapter {
    /**
     * @param {Object} options
     * @param {string} [options.dir] Directory to store per-guild JSON files.
     *   Default `<cwd>/riffy-state/`.
     * @param {import("../Riffy").Riffy} [options.riffy] Optional, for debug logging.
     */
    constructor(options = {}, riffy = null) {
        super();
        this.dir = (options && options.dir) || path.join(process.cwd(), "riffy-state");
        this.riffy = (options && options.riffy) || riffy || null;
    }

    _log(msg) {
        if (this.riffy) this.riffy.emit("debug", `[ShardedJsonStorage] ${msg}`);
    }

    /** @private Sanitize a guildId into a safe filename (no path traversal). */
    _pathFor(guildId) {
        // Only allow alphanumerics + dash/underscore to prevent `../` attacks.
        const safe = String(guildId).replace(/[^a-zA-Z0-9_-]/g, "");
        return path.join(this.dir, `${safe}.json`);
    }

    async init() {
        try {
            if (!fs.existsSync(this.dir)) fs.mkdirSync(this.dir, { recursive: true });
        } catch (e) {
            this._log(`init() failed to create dir ${this.dir}: ${e.message}`);
        }
    }

    async loadAll() {
        const map = new Map();
        let files = [];
        try {
            files = fs.readdirSync(this.dir).filter(f => f.endsWith(".json"));
        } catch (e) {
            this._log(`loadAll() couldn't read dir ${this.dir}: ${e.message}`);
            return map;
        }
        for (const file of files) {
            const fullPath = path.join(this.dir, file);
            try {
                const state = JSON.parse(fs.readFileSync(fullPath, "utf-8"));
                if (state && typeof state === "object" && state.guildId) {
                    map.set(state.guildId, state);
                }
            } catch (e) {
                this._log(`Skipping corrupt file ${file}: ${e.message}`);
                // Move the corrupt file aside.
                try {
                    fs.renameSync(fullPath, fullPath + ".corrupt-" + Date.now());
                } catch (_) { /* ignore */ }
            }
        }
        return map;
    }

    async save(guildId, state) {
        const fullPath = this._pathFor(guildId);
        // Atomic write: temp file in the same dir, then rename.
        const tmp = fullPath + ".tmp-" + process.pid;
        try {
            fs.writeFileSync(tmp, JSON.stringify(state, null, 2), "utf-8");
            fs.renameSync(tmp, fullPath);
        } catch (e) {
            this._log(`save() failed for ${guildId}: ${e.message}`);
            try { if (fs.existsSync(tmp)) fs.unlinkSync(tmp); } catch (_) { /* ignore */ }
            throw e;
        }
    }

    async remove(guildId) {
        const fullPath = this._pathFor(guildId);
        try {
            if (fs.existsSync(fullPath)) fs.unlinkSync(fullPath);
        } catch (e) {
            this._log(`remove() failed for ${guildId}: ${e.message}`);
        }
    }

    async clear() {
        try {
            if (!fs.existsSync(this.dir)) return;
            const files = fs.readdirSync(this.dir).filter(f => f.endsWith(".json"));
            for (const f of files) {
                try { fs.unlinkSync(path.join(this.dir, f)); } catch (_) { /* ignore */ }
            }
        } catch (e) {
            this._log(`clear() failed: ${e.message}`);
        }
    }

    async close() { /* no resources to release */ }
}

module.exports = { ShardedJsonStorage };
