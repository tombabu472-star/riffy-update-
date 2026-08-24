const path = require("node:path");
const { Track } = require("./Track");
const { StorageAdapter } = require("./storage/StorageAdapter");
const { JsonFileStorage } = require("./storage/JsonFileStorage");
const { ShardedJsonStorage } = require("./storage/ShardedJsonStorage");
const { SqliteStorage } = require("./storage/SqliteStorage");

/**
 * ResumeManager
 *
 * Provides client-restart auto-resume for Riffy. Persists player state
 * (voice channel, text channel, current track, playback position, volume,
 * loop mode, paused state, and the full queue) to a pluggable storage
 * adapter and restores all players after the bot process restarts.
 *
 * On restore it will:
 *   1. Re-create the player connection (rejoin the same voice channel).
 *   2. Rebuild the queue from the persisted encoded track strings.
 *   3. Re-send the current track to the Lavalink node at the last known
 *      position (seek), restoring volume and paused state.
 *
 * This is distinct from Node-level `autoResume` (which only re-establishes
 * players when the Lavalink WebSocket reconnects). ResumeManager survives a
 * full process restart.
 *
 * ## Scaling
 *
 * The storage backend is swappable via `options.storage`. Built-in adapters:
 *   - `"json"` / `{ type: "json", filePath }` — single file (default; <~100 players)
 *   - `"sharded"` / `{ type: "sharded", dir }` — one file per guild (thousands)
 *   - `"sqlite"` / `{ type: "sqlite", path }` — indexed DB (tens of thousands)
 *   - or pass any custom `StorageAdapter` instance (Redis, Postgres, Mongo…)
 *
 * If omitted, defaults to `JsonFileStorage` using `filePath` (backward compat).
 *
 * Per-guild writes are debounced independently — a busy guild never blocks
 * writes for other guilds.
 */
class ResumeManager {
    /**
     * @param {import("./Riffy").Riffy} riffy
     * @param {Object} [options]
     * @param {boolean} [options.enabled=false] Enable client-restart resume.
     * @param {string} [options.filePath] Path to the JSON file (JsonFileStorage) or directory (ShardedJsonStorage).
     * @param {number} [options.saveInterval=3000] Debounce window (ms) for per-guild disk writes.
     * @param {(requester: any) => any} [options.requesterResolver] Optional function to rebuild a requester object from the persisted value.
     * @param {boolean} [options.clearOnRestore=false] Delete persisted state after a successful full restore.
     * @param {number} [options.restoreTimeout=15000] Max ms to wait for voice credentials per player before aborting.
     * @param {number|null} [options.maxQueueSize=null] Cap how many queue tracks to persist per guild (prevents a single huge playlist from bloating storage). `null` = no limit.
     * @param {string|object|StorageAdapter} [options.storage] Storage adapter: preset string ("json"|"sharded"|"sqlite"), a config object ({type, ...}), or a custom adapter instance.
     */
    constructor(riffy, options = {}) {
        this.riffy = riffy;
        this.enabled = options.enabled ?? false;
        this.filePath = options.filePath || path.join(process.cwd(), "riffy-state.json");
        this.saveInterval = typeof options.saveInterval === "number" ? options.saveInterval : 3000;
        this.requesterResolver = typeof options.requesterResolver === "function" ? options.requesterResolver : null;
        this.clearOnRestore = options.clearOnRestore ?? false;
        this.restoreTimeout = typeof options.restoreTimeout === "number" ? options.restoreTimeout : 15000;
        /**
         * Max number of queue tracks to persist per guild. A bot in a guild
         * where someone queued a 500-track playlist would otherwise bloat
         * storage; capping prevents that. `null` = no limit.
         * Default: `null` (no limit).
         * @since 1.0.15
         */
        this.maxQueueSize = options.maxQueueSize ?? null;
        /**
         * Max number of players to restore in parallel. Each restorePlayer
         * involves a per-guild voice handshake that can take up to
         * restoreTimeout. Restoring sequentially would stack these windows
         * (50 guilds × 15s = 12.5 min worst case). Bounded concurrency keeps
         * restores parallel without overwhelming the Discord gateway.
         * Default: 8.
         * @since 1.0.15
         */
        this.restoreConcurrency = typeof options.restoreConcurrency === "number" ? options.restoreConcurrency : 8;

        /** @type {{ version: number, savedAt: number, players: Record<string, any> }} */
        this._state = { version: 1, savedAt: 0, players: {} };
        this._restored = false;
        this._restoredFully = false;
        this._listenersAttached = false;
        /** @type {Map<string, (reason: string, detail?: any) => void>} guildId -> reject fn for active restores */
        this._pendingRestores = new Map();
        /** @type {Map<string, NodeJS.Timeout>} guildId -> per-guild debounce timer */
        this._saveTimers = new Map();
        /** @type {Promise<boolean> | null} load() promise (in-flight or null). */
        this._loadPromise = null;
        /** @private Stored process signal handler refs so clear()/destroy() can unregister them. */
        this._processHandlers = null;
        /** @private Stored riffy event handler refs so destroy() can unregister them. */
        this._riffyHandlers = null;

        /** @type {StorageAdapter} */
        this.storage = this._resolveStorage(options);
    }

    /**
     * Resolve the storage adapter from the `storage` option, falling back to
     * JsonFileStorage for backward compatibility.
     * @private
     */
    _resolveStorage(options) {
        const s = options.storage;
        // 1. Custom adapter instance — use as-is.
        if (s && typeof s === "object" && typeof s.save === "function" && typeof s.loadAll === "function") {
            return s;
        }
        // 2. Config object: { type: "json"|"sharded"|"sqlite", ... }
        if (s && typeof s === "object" && typeof s.type === "string") {
            if (s.type === "json") return new JsonFileStorage({ filePath: s.filePath || options.filePath }, this.riffy);
            if (s.type === "sharded") return new ShardedJsonStorage({ dir: s.dir || options.filePath || path.join(process.cwd(), "riffy-state") }, this.riffy);
            if (s.type === "sqlite") return new SqliteStorage({ path: s.path || options.filePath || "riffy-state.db" }, this.riffy);
            throw new Error(`Unknown storage type: ${s.type}. Use "json", "sharded", "sqlite", or pass a custom StorageAdapter.`);
        }
        // 3. Preset string: "json" | "sharded" | "sqlite"
        if (typeof s === "string") {
            if (s === "json") return new JsonFileStorage({ filePath: options.filePath }, this.riffy);
            if (s === "sharded") return new ShardedJsonStorage({ dir: options.filePath || path.join(process.cwd(), "riffy-state") }, this.riffy);
            if (s === "sqlite") return new SqliteStorage({ path: options.filePath || "riffy-state.db" }, this.riffy);
            throw new Error(`Unknown storage preset: ${s}. Use "json", "sharded", or "sqlite".`);
        }
        // 4. Default (backward compat): single JSON file via filePath.
        return new JsonFileStorage({ filePath: options.filePath }, this.riffy);
    }

    /**
     * Load persisted state from the storage adapter into memory. Async (the
     * adapter may do real I/O). Returns a Promise that resolves to the loaded
     * Map; callers that don't await can rely on restoreAll() to await it.
     *
     * Corrupt entries are dropped + logged by the adapter rather than crashing
     * the restore. Validation is also applied here as a second line of defense.
     *
     * @returns {Promise<boolean>} Whether any valid players were loaded.
     */
    load() {
        if (!this.enabled) return Promise.resolve(false);
        this._loadPromise = (async () => {
            await this.storage.init();
            const loaded = await this.storage.loadAll();
            const validPlayers = {};
            let dropped = 0;
            for (const [guildId, state] of loaded.entries()) {
                const validation = this._validatePlayerState(guildId, state);
                if (validation.ok) {
                    validPlayers[guildId] = state;
                } else {
                    dropped++;
                    this.riffy.emit("debug", `[ResumeManager] Dropped invalid persisted entry for guild ${guildId}: ${validation.reason}`);
                    // Remove the bad entry from the store so it isn't retried.
                    this.storage.remove(guildId).catch(() => {});
                }
            }
            this._state = { version: 1, savedAt: Date.now(), players: validPlayers };
            const count = Object.keys(validPlayers).length;
            this.riffy.emit("debug", `[ResumeManager] Loaded state for ${count} player(s) via ${this.storage.constructor.name}${dropped ? ` (dropped ${dropped} invalid entry/entries)` : ""}`);
            return count > 0;
        })().catch((e) => {
            this.riffy.emit("debug", `[ResumeManager] load() failed: ${e.message}`);
            return false;
        });
        return this._loadPromise;
    }

    /**
     * Validate a single persisted player state object. Guards against partial
     * writes / storage corruption producing malformed entries that would
     * crash _rebuildTrack or restorePlayer.
     * @private
     * @returns {{ ok: boolean, reason?: string }}
     */
    _validatePlayerState(guildId, state) {
        if (!state || typeof state !== "object") return { ok: false, reason: "not an object" };
        if (typeof state.guildId !== "string" || !state.guildId) return { ok: false, reason: "missing guildId" };
        // Cross-check: the stored state's guildId must match the key it was
        // stored under. A mismatch indicates corruption or a storage bug.
        if (guildId && state.guildId !== guildId) return { ok: false, reason: `guildId mismatch (key=${guildId}, state=${state.guildId})` };
        if (typeof state.voiceChannel !== "string" || !state.voiceChannel) return { ok: false, reason: "missing voiceChannel" };
        if (state.volume !== undefined && typeof state.volume !== "number") return { ok: false, reason: "volume is not a number" };
        if (state.position !== undefined && typeof state.position !== "number") return { ok: false, reason: "position is not a number" };
        if (state.paused !== undefined && typeof state.paused !== "boolean") return { ok: false, reason: "paused is not a boolean" };
        if (state.queue !== undefined && !Array.isArray(state.queue)) return { ok: false, reason: "queue is not an array" };
        if (state.current !== undefined && state.current !== null && typeof state.current !== "object") return { ok: false, reason: "current is not an object" };
        if (state.current && state.current.encoded !== undefined && state.current.encoded !== null && typeof state.current.encoded !== "string") return { ok: false, reason: "current.encoded is not a string" };
        if (state.queue) {
            for (let i = 0; i < state.queue.length; i++) {
                const t = state.queue[i];
                if (!t || typeof t !== "object") return { ok: false, reason: `queue[${i}] is not an object` };
                if (t.encoded !== undefined && t.encoded !== null && typeof t.encoded !== "string") return { ok: false, reason: `queue[${i}].encoded is not a string` };
            }
        }
        return { ok: true };
    }

    /**
     * Flush any pending per-guild debounced writes immediately. Returns a
     * Promise that resolves when all pending saves have settled.
     * @returns {Promise<void>}
     */
    async save() {
        if (!this.enabled) return;
        const guildIds = [...this._saveTimers.keys()];
        await Promise.allSettled(guildIds.map((guildId) => {
            const timer = this._saveTimers.get(guildId);
            if (timer) { clearTimeout(timer); this._saveTimers.delete(guildId); }
            const state = this._state.players[guildId];
            if (state) return this.storage.save(guildId, state);
            return this.storage.remove(guildId);
        }));
    }

    /**
     * Safely serialize a Track's info into a JSON-safe plain object.
     * The `requester` is reduced to a primitive/id (it is usually a Discord
     * User object which is not JSON-serializable due to circular refs).
     *
     * Field names match Lavalink v4's raw track info (isSeekable, isStream)
     * so the serialized object can be passed directly to `new Track({ info })`
     * on restore — the Track constructor reads info.isSeekable / info.isStream.
     * The Track class's runtime info object uses seekable/stream (mapped from
     * the Lavalink names), so we read those here and write the Lavalink names.
     * @private
     */
    _safeSerializeInfo(info) {
        if (!info) return null;
        const out = {
            identifier: info.identifier ?? null,
            isSeekable: info.seekable ?? info.isSeekable ?? false,
            author: info.author ?? null,
            length: info.length ?? 0,
            isStream: info.stream ?? info.isStream ?? false,
            position: info.position ?? 0,
            title: info.title ?? null,
            uri: info.uri ?? null,
            sourceName: info.sourceName ?? null,
            isrc: info.isrc ?? null,
            artworkUrl: null,
        };
        // Resolve thumbnail via the getter (returns a URL string or null).
        try {
            const thumb = typeof info.thumbnail === "string" ? info.thumbnail : (info._cachedThumbnail ?? null);
            out.artworkUrl = thumb;
        } catch (_) {
            out.artworkUrl = null;
        }
        // Persist requester only in a serializable form.
        const req = info.requester;
        if (req !== undefined && req !== null) {
            if (typeof req === "string" || typeof req === "number" || typeof req === "boolean") {
                out.requester = req;
            } else if (typeof req === "object" && req.id) {
                out.requester = req.id;
            } else {
                out.requester = null;
            }
        } else {
            out.requester = null;
        }
        return out;
    }

    /**
     * Serialize a player into a JSON-safe object. If `maxQueueSize` is set,
     * the queue is truncated to that many tracks (keeping the first N, which
     * are the soonest-to-play) to prevent a single huge playlist from
     * bloating storage.
     * @param {import("./Player").Player} player
     */
    serializePlayer(player) {
        if (!player || !player.guildId) return null;

        const current = player.current
            ? {
                  encoded: player.current.track || player.current.encoded || null,
                  info: this._safeSerializeInfo(player.current.info),
                  pluginInfo: player.current.pluginInfo || null,
              }
            : null;

        let queue = (player.queue || []).map((t) => ({
            encoded: t.track || t.encoded || null,
            info: this._safeSerializeInfo(t.info),
            pluginInfo: t.pluginInfo || null,
        }));
        // Cap queue size to bound per-guild storage.
        if (this.maxQueueSize !== null && queue.length > this.maxQueueSize) {
            queue = queue.slice(0, this.maxQueueSize);
        }

        return {
            guildId: player.guildId,
            voiceChannel: player.voiceChannel ?? null,
            textChannel: player.textChannel ?? null,
            volume: player.volume ?? 100,
            loop: player.loop ?? "none",
            paused: player.paused ?? false,
            playing: player.playing ?? false,
            position: player.position || 0,
            deaf: player.deaf ?? true,
            mute: player.mute ?? false,
            current,
            queue,
            savedAt: Date.now(),
        };
    }

    /**
     * Update stored state for a single player and schedule a per-guild
     * debounced write. Each guild has its own timer, so a busy guild never
     * delays writes for other guilds.
     * @param {import("./Player").Player} player
     */
    savePlayer(player) {
        if (!this.enabled) return;
        const serialized = this.serializePlayer(player);
        if (!serialized) return;
        this._state.players[player.guildId] = serialized;
        // Per-guild debounce.
        if (this._saveTimers.has(player.guildId)) {
            clearTimeout(this._saveTimers.get(player.guildId));
        }
        this._saveTimers.set(player.guildId, setTimeout(() => {
            this._saveTimers.delete(player.guildId);
            const state = this._state.players[player.guildId];
            if (state) {
                this.storage.save(player.guildId, state).catch((err) => {
                    this.riffy.emit("debug", `[ResumeManager] storage.save failed for ${player.guildId}: ${err.message}`);
                });
            }
        }, this.saveInterval));
    }

    /**
     * Remove a player from persisted state immediately (no debounce —
     * destroys should be instant so the guild isn't restored next restart).
     * @param {string} guildId
     */
    removePlayer(guildId) {
        if (!this.enabled) return;
        // Cancel any pending save for this guild.
        if (this._saveTimers.has(guildId)) {
            clearTimeout(this._saveTimers.get(guildId));
            this._saveTimers.delete(guildId);
        }
        if (this._state.players[guildId]) {
            delete this._state.players[guildId];
            this.storage.remove(guildId).catch((err) => {
                this.riffy.emit("debug", `[ResumeManager] storage.remove failed for ${guildId}: ${err.message}`);
            });
        }
    }

    /**
     * Reconstruct a Track object from serialized data. Async because the
     * requesterResolver may return a Promise (e.g. client.users.fetch(id)).
     * @private
     * @param {any} data Serialized track data.
     * @param {any} node The Lavalink node to pass to the Track constructor.
     * @param {string} [guildId] The guild being restored (for debug logging).
     */
    async _rebuildTrack(data, node, guildId) {
        if (!data) return null;
        const encoded = data.encoded || null;
        let requester = data.info?.requester ?? null;
        if (requester !== null && this.requesterResolver) {
            try {
                const resolved = this.requesterResolver(requester);
                // Support both sync and async resolvers — await a Promise,
                // use the value directly otherwise. Fall back to the
                // primitive requester if the resolver returns null/undefined.
                if (resolved instanceof Promise) {
                    requester = (await resolved) ?? requester;
                } else {
                    requester = resolved ?? requester;
                }
            } catch (e) {
                // Resolver threw (e.g. user not found) — keep primitive requester.
                this.riffy.emit("debug", `[ResumeManager] requesterResolver threw for guild ${guildId ?? "?"}: ${e.message}`);
            }
        }
        const track = new Track(
            { encoded, info: data.info || {}, pluginInfo: data.pluginInfo || {} },
            requester,
            node
        );
        return track;
    }

    /**
     * Restore all players from persisted state. Should be called after Riffy
     * is initialized and at least one node has emitted "ready".
     *
     * If `clearOnRestore` is `true`, the persisted state file is deleted from
     * disk once the restore completes (whether or not all players were
     * restored successfully — failed entries are dropped from the in-memory
     * state, and the file is removed so it can't be re-applied on the next
     * restart). New player activity after this point is persisted fresh.
     *
     * @returns {Promise<Array<import("./Player").Player>>}
     */
    async restoreAll() {
        if (!this.enabled) return [];
        // Guard: restoreAll is idempotent — once it has run (even partially),
        // it won't run again unless load() is called to reload state. This
        // prevents duplicate restores if init()'s nodeConnect handler fires
        // multiple times or resumePlayers() is called manually after init.
        if (this._restored) {
            this.riffy.emit("debug", `[ResumeManager] restoreAll() already ran, skipping (call load() to reload state first).`);
            return [];
        }
        this._restored = true;

        // Await any pending load (the adapter may still be reading).
        if (this._loadPromise) {
            try { await this._loadPromise; } catch (_) { /* already logged */ }
        }

        const guildIds = Object.keys(this._state.players || {});
        if (!guildIds.length) {
            this.riffy.emit("debug", `[ResumeManager] No saved players to restore.`);
            if (this.clearOnRestore) {
                await this.storage.clear().catch(() => {});
                this._restoredFully = true;
            }
            return [];
        }

        this.riffy.emit("debug", `[ResumeManager] Restoring ${guildIds.length} player(s) (concurrency=${this.restoreConcurrency})...`);
        const restored = [];

        // Restore players concurrently with a bounded concurrency limit so a
        // bot in 50+ guilds doesn't restore sequentially (each restoreTimeout
        // window would otherwise stack: 50 × 15s = 12.5 min worst case).
        // Each restorePlayer is independent (per-guild voice handshakes), so
        // they can run in parallel safely.
        const concurrency = Math.max(1, this.restoreConcurrency);
        const queue = [...guildIds];
        const workers = [];
        const worker = async () => {
            while (queue.length > 0) {
                const guildId = queue.shift();
                const state = this._state.players[guildId];
                if (!state) continue;
                try {
                    const player = await this.restorePlayer(state);
                    if (player) restored.push(player);
                } catch (e) {
                    this.riffy.emit("debug", `[ResumeManager] Failed to restore player ${guildId}: ${e.message}`);
                    delete this._state.players[guildId];
                }
            }
        };
        for (let i = 0; i < Math.min(concurrency, guildIds.length); i++) {
            workers.push(worker());
        }
        await Promise.allSettled(workers);

        this.riffy.emit("debug", `[ResumeManager] Restore complete (${restored.length}/${guildIds.length} succeeded).`);

        if (this.clearOnRestore) {
            // Wipe in-memory state and clear the store so it isn't re-applied
            // on the next restart. Fresh state is written as new activity happens.
            this._state = { version: 1, savedAt: 0, players: {} };
            for (const t of this._saveTimers.values()) clearTimeout(t);
            this._saveTimers.clear();
            await this.storage.clear().catch(() => {});
            this._restoredFully = true;
            this.riffy.emit("debug", `[ResumeManager] clearOnRestore enabled — store cleared, in-memory state wiped.`);
        }

        return restored;
    }

    /**
     * Restore a single player from serialized state. Wraps the whole flow in
     * a timeout (`restoreTimeout`); if Discord doesn't grant voice credentials
     * in time (channel deleted, bot kicked, missing Connect permission), the
     * restore is aborted, the half-created player is destroyed, the guild is
     * removed from persisted state, and `playerRestoreFailed` is emitted.
     *
     * @param {any} state
     * @returns {Promise<import("./Player").Player | null>}
     */
    async restorePlayer(state) {
        if (!state || !state.guildId || !state.voiceChannel) {
            await this._failRestore(state?.guildId ?? null, "invalid_state", "Missing guildId or voiceChannel", state);
            return null;
        }

        // Skip if a player already exists for this guild.
        if (this.riffy.players.has(state.guildId)) {
            this.riffy.emit("debug", `[ResumeManager] Player already exists for ${state.guildId}, skipping restore.`);
            return this.riffy.players.get(state.guildId);
        }

        const node = this.riffy.bestNode || this.riffy.leastUsedNodes[0];
        if (!node) {
            await this._failRestore(state.guildId, "no_nodes", "No connected nodes available to restore player", state);
            return null;
        }

        const guildId = state.guildId;

        // Set up a promise that rejects on timeout OR on socketClosed during
        // the restore window. This lets us abort early instead of always
        // waiting the full restoreTimeout.
        let settleTimer = null;
        let socketClosedHandler = null;
        // Abort flag: set by the reject function (timeout / socketClosed) so
        // the async executor still running in _sendResumePayload can bail out
        // instead of PATCHing Lavalink + emitting playerResumed AFTER the
        // restore has already been treated as failed. Without this, a late
        // VOICE_SERVER_UPDATE would let the executor resurrect a destroyed
        // player (race condition flagged in review).
        let aborted = false;
        const isAborted = () => aborted;

        // Register the reject fn BEFORE creating the Promise so timeout /
        // socketClosed can abort it. This avoids the `new Promise(async ...)`
        // anti-pattern where errors in the executor's setup (e.g. Map.set
        // throwing) would be swallowed and the Promise would never settle.
        const rejectRef = { fn: null };
        this._pendingRestores.set(guildId, (reason, detail) => {
            aborted = true;
            if (rejectRef.fn) rejectRef.fn({ reason, detail });
        });

        const runRestore = async () => {
            // 1. Re-create the connection -> sends VOICE_STATE_UPDATE to rejoin.
            const player = this.riffy.createConnection({
                guildId,
                voiceChannel: state.voiceChannel,
                textChannel: state.textChannel,
                deaf: state.deaf ?? true,
                mute: state.mute ?? false,
                defaultVolume: state.volume ?? 100,
                loop: state.loop ?? "none",
            });

            // 2. Restore volume & loop mode.
            if (typeof state.volume === "number") player.volume = state.volume;
            if (state.loop) player.loop = state.loop;

            // 3. Rebuild the queue. Use a bounded-concurrency batch pattern so
            // an async requesterResolver (e.g. client.users.fetch) doesn't
            // serialize N network calls. Aborts are checked between batches.
            if (Array.isArray(state.queue) && !isAborted()) {
                const BATCH_SIZE = 10;
                for (let i = 0; i < state.queue.length; i += BATCH_SIZE) {
                    if (isAborted()) break;
                    const batch = state.queue.slice(i, i + BATCH_SIZE);
                    const rebuiltTracks = await Promise.all(
                        batch.map((t) => isAborted() ? null : this._rebuildTrack(t, node, guildId))
                    );
                    for (const rebuilt of rebuiltTracks) {
                        if (rebuilt) player.queue.add(rebuilt);
                    }
                }
            }

            // 4. Restore current track + seek to the saved position.
            if (!isAborted() && state.current && (state.current.encoded || (state.current.info && state.current.info.identifier))) {
                const currentTrack = await this._rebuildTrack(state.current, node, guildId);
                player.current = currentTrack;
                player.position = state.position || 0;
                player.paused = state.paused ?? false;

                // Send the resume payload once voice credentials arrive.
                // Pass isAborted so it can bail out if voice creds arrive
                // AFTER a timeout/socketClosed already aborted the restore.
                await this._sendResumePayload(player, state, isAborted);
            } else if (!isAborted() && player.queue.length > 0) {
                // No current track but queue has items — start playback.
                // If this fails (e.g. voice init failed, track unresolvable),
                // the error MUST propagate so the .catch below calls
                // _failRestore → destroys the player, removes it from state,
                // and emits playerRestoreFailed.
                await player.play();
            }

            // If the restore was aborted while we were waiting, do NOT
            // emit the success debug line — the promise has already been
            // rejected.
            if (isAborted()) {
                return null;
            }

            this.riffy.emit(
                "debug",
                `[ResumeManager] Restored player for guild ${guildId} (voice=${state.voiceChannel}, queue=${player.queue.length}, position=${state.position || 0}ms, paused=${state.paused ?? false})`
            );
            return player;
        };

        const restorePromise = new Promise((resolve, reject) => {
            rejectRef.fn = reject;
            runRestore().then(
                (player) => { if (player) resolve(player); },
                (err) => { if (!isAborted()) reject({ reason: "error", detail: err }); }
            );
        });

        // Timeout: if voice credentials never arrive, abort.
        settleTimer = setTimeout(() => {
            const rejectFn = this._pendingRestores.get(guildId);
            if (rejectFn) {
                this.riffy.emit("debug", `[ResumeManager] Restore timed out for ${guildId} after ${this.restoreTimeout}ms (channel deleted / no permission / bot kicked?).`);
                rejectFn("timeout", `Voice credentials not received within ${this.restoreTimeout}ms`);
            }
        }, this.restoreTimeout);

        // Listen for socketClosed during the restore window — Discord sends
        // these when the bot can't connect (permission denied, channel gone).
        socketClosedHandler = (player, payload) => {
            if (player.guildId !== guildId) return;
            // Codes that indicate the bot can't join:
            //   4006 = session invalid, 4014 = server not available / disconnected,
            //   4009 = session timeout, 4015 = server crash
            const fatalCodes = [4006, 4009, 4014, 4015];
            if (fatalCodes.includes(payload.code)) {
                const rejectFn = this._pendingRestores.get(guildId);
                if (rejectFn) {
                    this.riffy.emit("debug", `[ResumeManager] Voice socket closed (code ${payload.code}) during restore for ${guildId}, aborting.`);
                    rejectFn("socket_closed", `Voice socket closed: code ${payload.code} (${payload.reason || "no reason"})`);
                }
            }
        };
        // Bump the maxListeners limit before adding a concurrent socketClosed
        // listener. With restoreConcurrency=8 (default), up to 8 listeners are
        // registered simultaneously. Node's default EventEmitter limit is 10,
        // so any bot with other socketClosed listeners would hit
        // MaxListenersExceededWarning. We restore the limit in the finally block.
        const prevMaxListeners = this.riffy.getMaxListeners();
        if (prevMaxListeners !== 0) { // 0 = unlimited, no need to bump
            this.riffy.setMaxListeners(prevMaxListeners + 1);
        }
        this.riffy.on("socketClosed", socketClosedHandler);

        try {
            const player = await restorePromise;
            return player;
        } catch (err) {
            const reason = err?.reason || "error";
            const detail = err?.detail instanceof Error ? err.detail.message : (err?.detail || "unknown");
            await this._failRestore(guildId, reason, detail, state);
            return null;
        } finally {
            clearTimeout(settleTimer);
            this._pendingRestores.delete(guildId);
            if (socketClosedHandler) this.riffy.off("socketClosed", socketClosedHandler);
            // Restore the maxListeners limit we bumped above.
            if (prevMaxListeners !== 0) {
                this.riffy.setMaxListeners(Math.max(0, this.riffy.getMaxListeners() - 1));
            }
        }
    }

    /**
     * Handle a failed restore: destroy any half-created player, remove the
     * guild from persisted state, and emit `playerRestoreFailed`.
     *
     * Sends an explicit, AWAITED DELETE to Lavalink (not just
     * player.destroy(), which dispatches destroyPlayer() without awaiting
     * it). This ensures the DELETE is ordered relative to any in-flight
     * PATCH from _sendResumePayload — if the PATCH lands AFTER this DELETE,
     * _sendResumePayload's post-PATCH orphan-cleanup DELETE removes the
     * recreated Lavalink player.
     *
     * @private
     */
    async _failRestore(guildId, reason, detail, state) {
        if (!guildId) return;

        // Destroy the half-created player if it exists. We send an explicit,
        // AWAITED DELETE to Lavalink here because Player.destroy() calls
        // node.rest.destroyPlayer() WITHOUT awaiting it (the promise is
        // discarded). That matters for this race: if a PATCH from
        // _sendResumePayload is in flight when this fires, an unawaited
        // DELETE could complete before OR after the PATCH lands — and if
        // the PATCH lands AFTER the DELETE, it recreates an orphan Lavalink
        // player. By awaiting our own DELETE here, the local cleanup is
        // ordered relative to any PATCH, and _sendResumePayload's
        // post-PATCH orphan-cleanup DELETE handles the other ordering.
        const player = this.riffy.players.get(guildId);
        if (player) {
            // Send an awaited DELETE to Lavalink first (local cleanup below).
            try {
                await player.node.rest.destroyPlayer(guildId);
            } catch (e) {
                this.riffy.emit("debug", `[ResumeManager] destroyPlayer DELETE failed for ${guildId}: ${e.message}`);
            }
            try {
                // skipRest=true: we already sent the DELETE above, so don't
                // let player.destroy() fire a second (unawaited) one.
                player.destroy(true);
            } catch (e) {
                this.riffy.emit("debug", `[ResumeManager] Error destroying failed player for ${guildId}: ${e.message}`);
            }
            // Safety net: always remove from the players map, even if
            // destroy() threw partway through and never reached the
            // internal `this.riffy.players.delete(guildId)` line.
            this.riffy.players.delete(guildId);
        }

        // Remove from persisted state so it isn't retried on every restart.
        // Note: player.destroy(true) above emits playerDisconnect → the
        // attachListeners hook already called removePlayer(guildId) which
        // deletes from _state.players + calls storage.remove(). This block
        // is a safety net for the case where the hook isn't attached (e.g.
        // attachListeners wasn't called, or destroy() didn't emit the event).
        // The guard prevents a redundant storage.remove() call when
        // clearOnRestore would wipe everything anyway.
        if (this._state.players[guildId]) {
            delete this._state.players[guildId];
            if (!this.clearOnRestore) {
                this.storage.remove(guildId).catch((err) => {
                    this.riffy.emit("debug", `[ResumeManager] storage.remove failed for ${guildId} after failed restore: ${err.message}`);
                });
            }
        }

        this.riffy.emit("debug", `[ResumeManager] Restore FAILED for guild ${guildId}: reason="${reason}", detail="${detail}". Player destroyed, state removed.`);

        /**
         * @event playerRestoreFailed
         * Emitted when a player could not be restored after a client restart.
         * @param {string} guildId
         * @param {string} reason One of: "timeout" | "socket_closed" | "no_nodes" | "invalid_state" | "error"
         * @param {string} detail Human-readable detail.
         * @param {any} state The persisted state that failed to restore.
         */
        this.riffy.emit("playerRestoreFailed", guildId, reason, detail, state);
    }

    /**
     * Wait for voice credentials then send the track + seek position to the node.
     *
     * IMPORTANT: If connection.resolve() rejects (voice credentials never
     * arrived — channel deleted, bot kicked, no Connect permission), the
     * error is re-thrown so restorePlayer's catch block calls _failRestore.
     * We must NOT swallow it and proceed to PATCH the track + emit
     * playerResumed — that would treat a failed restore as successful.
     *
     * RACE FIX 1: If the restore was aborted (timeout / socketClosed) WHILE
     * we're awaiting connection.resolve(), the caller's _failRestore has
     * already destroyed the player + emitted playerRestoreFailed. If voice
     * creds then arrive, we must NOT PATCH Lavalink or emit playerResumed —
     * that would resurrect a destroyed player. The `isAborted` callback is
     * checked after the await and before the PATCH to bail out cleanly.
     *
     * RACE FIX 2: If the abort fires WHILE the PATCH is in flight, the
     * PATCH may land on Lavalink AFTER _failRestore's DELETE (sent via
     * player.destroy()), recreating an orphan Lavalink player that keeps
     * playing. So after the PATCH resolves, if aborted, we send a DELETE
     * to clean up any orphan Lavalink player the PATCH may have created.
     * _failRestore also now awaits player.destroy() so the DELETE is
     * ordered relative to the PATCH.
     *
     * RACE FIX 3 (replacement player): the orphan-cleanup DELETE is
     * guild-scoped, but by the time it fires, _failRestore has already
     * emitted playerRestoreFailed. If the user's handler created a
     * REPLACEMENT player for the same guild, this.players.get(guildId)
     * now points at a DIFFERENT instance. A blind guild-scoped DELETE
     * would kill the replacement's Lavalink session. So we only send the
     * orphan-cleanup DELETE if the player instance we PATCHed is still
     * the one registered for that guild (i.e. no replacement exists).
     *
     * @private
     * @param {import("./Player").Player} player The player being resumed (captured by ref).
     * @param {() => boolean} [isAborted] Returns true if the restore was aborted.
     */
    async _sendResumePayload(player, state, isAborted = () => false) {
        // This throws if Discord doesn't supply voice credentials. Let it
        // propagate so the restore is treated as a failure, not a success.
        await player.connection.resolve();

        // RACE FIX 1: voice creds arrived, but the restore may have already
        // been aborted (timeout / socketClosed) while we were waiting. If so,
        // _failRestore has already destroyed the player + emitted
        // playerRestoreFailed. Bail out — do NOT PATCH or emit playerResumed.
        if (isAborted()) {
            this.riffy.emit("debug", `[ResumeManager] Voice credentials arrived for ${player.guildId} but restore was already aborted; not patching/emitting.`);
            return;
        }

        const encoded = state.current?.encoded || player.current?.track || player.current?.encoded;
        if (!encoded) {
            // No encoded track to resume — throw so restorePlayer's catch
            // calls _failRestore (destroys the player, emits
            // playerRestoreFailed) instead of falling through to
            // resolve(player) + emit playerResumed, which would be a false
            // success (bot rejoins voice channel but produces no audio).
            throw new Error(`No encoded track to resume for ${player.guildId}`);
        }

        await player.node.rest.updatePlayer({
            guildId: player.guildId,
            data: {
                track: { encoded },
                position: state.position || 0,
                volume: state.volume ?? player.volume,
                paused: state.paused ?? false,
            },
        });

        // RACE FIX 2 + 3: if aborted DURING/AFTER the PATCH, the PATCH may
        // have landed on Lavalink AFTER _failRestore's DELETE, recreating an
        // orphan Lavalink player. Send a DELETE to clean it up — UNLESS a
        // REPLACEMENT player now exists for this guild (which would be killed
        // by a guild-scoped DELETE).
        //
        // Three cases after the PATCH resolves and isAborted() is true:
        //   1. currentPlayer === player        → original still registered, no
        //      replacement. DELETE the orphan.
        //   2. currentPlayer === undefined     → original was removed by
        //      _failRestore, no replacement. DELETE the orphan. (This was the
        //      bug: the old `=== player` check treated `undefined` as "a
        //      replacement exists" and skipped the DELETE, leaving an orphan
        //      Lavalink player playing after playerRestoreFailed.)
        //   3. currentPlayer !== player && !== undefined → a REPLACEMENT
        //      player exists. Skip the DELETE — it would kill the replacement.
        if (isAborted()) {
            const currentPlayer = this.riffy.players.get(player.guildId);
            const replacementExists = currentPlayer !== undefined && currentPlayer !== player;
            if (!replacementExists) {
                // No replacement (original still registered OR already removed) —
                // safe to delete the orphan we may have just recreated on Lavalink.
                this.riffy.emit("debug", `[ResumeManager] Restore aborted during/after PATCH for ${player.guildId}; sending DELETE to clean up orphan Lavalink player.`);
                try {
                    await player.node.rest.destroyPlayer(player.guildId);
                } catch (e) {
                    this.riffy.emit("debug", `[ResumeManager] Orphan-cleanup DELETE failed for ${player.guildId}: ${e.message}`);
                }
                // RACE FIX 4: a replacement player may have been created and
                // established its Lavalink session WHILE the DELETE was in flight
                // (the user's async playerRestoreFailed handler runs after
                // _failRestore emits the event, and createConnection →
                // updatePlayer can land before this DELETE completes). The
                // guild-scoped DELETE just killed the replacement's Lavalink
                // session — including its voice session. Re-send the replacement's
                // voice credentials AND track (if any) to fully restore the
                // Lavalink session.
                //
                // RACE FIX 5: the replacement's Connection records the voice
                // credentials as already sent (#lastSentVoice === current voice),
                // so it would skip re-sending them on a normal updatePlayer. We
                // must include the voice data explicitly in this recovery PATCH
                // so Lavalink re-establishes the voice connection — otherwise the
                // replacement has a track but no voice session and produces no
                // audio.
                //
                // RACE FIX 6: the replacement may have established voice
                // credentials but NOT yet assigned a current track (it just
                // joined the voice channel, hasn't started playing). The previous
                // `afterDelete.current` guard would skip the recovery PATCH
                // entirely in that case — but the DELETE still killed the
                // replacement's voice session, and without recovery the
                // replacement would remain registered locally with no audio and
                // no way to restore the voice session (Connection sees unchanged
                // creds as already sent). So the guard is now based on voice
                // credentials, not on a current track. The track is optional in
                // the recovery data.
                const afterDelete = this.riffy.players.get(player.guildId);
                const replConn = afterDelete?.connection;
                const hasVoice = !!(replConn && replConn.voice && replConn.voice.sessionId && replConn.voice.endpoint && replConn.voice.token);
                if (afterDelete && afterDelete !== player && hasVoice) {
                    this.riffy.emit("debug", `[ResumeManager] Replacement player detected after orphan-cleanup DELETE for ${player.guildId}; re-sending its${afterDelete.current ? " track +" : ""} voice credentials to restore the Lavalink session.`);
                    try {
                        // Build the recovery PATCH data. Track + playback fields
                        // are only included if the replacement has a current track.
                        const recoveryData = {};
                        if (afterDelete.current) {
                            const replEncoded = afterDelete.current.track || afterDelete.current.encoded;
                            if (replEncoded) {
                                recoveryData.track = { encoded: replEncoded };
                                recoveryData.position = afterDelete.position || 0;
                                recoveryData.volume = afterDelete.volume;
                                recoveryData.paused = afterDelete.paused;
                            }
                        }
                        // Voice credentials are always included when present —
                        // this is the critical part that restores the voice
                        // session the DELETE just destroyed. The replacement's
                        // Connection won't re-send them on its own because
                        // #lastSentVoice matches the current voice.
                        recoveryData.voice = {
                            sessionId: replConn.voice.sessionId,
                            endpoint: replConn.voice.endpoint,
                            token: replConn.voice.token,
                            channelId: replConn.voiceChannel ?? afterDelete.voiceChannel,
                        };
                        await afterDelete.node.rest.updatePlayer({
                            guildId: player.guildId,
                            data: recoveryData,
                        });
                    } catch (e) {
                        this.riffy.emit("debug", `[ResumeManager] Re-PATCH of replacement failed for ${player.guildId}: ${e.message}`);
                    }
                }
            } else {
                // A replacement player exists for this guild — do NOT send
                // a guild-scoped DELETE, it would kill the replacement's
                // Lavalink session.
                this.riffy.emit("debug", `[ResumeManager] Restore aborted after PATCH for ${player.guildId}, but a replacement player is now registered; skipping orphan-cleanup DELETE to avoid killing the replacement's Lavalink session.`);
            }
            return;
        }

        player.playing = !(state.paused ?? false);
        player.paused = state.paused ?? false;
        player.position = state.position || 0;

        this.riffy.emit("playerResumed", player);
    }

    /**
     * Attach event listeners to automatically persist state on player changes.
     *
     * Also registers process signal handlers (exit/SIGINT/SIGTERM) for a
     * best-effort final flush. These handlers are stored so {@link destroy}
     * can unregister them — important if the user re-creates ResumeManager
     * (e.g. calling init() with different options after a hot reload).
     */
    attachListeners() {
        if (!this.enabled || this._listenersAttached) return;
        this._listenersAttached = true;

        const riffy = this.riffy;

        // Store riffy event handler refs so destroy() can unregister them.
        const onTrackStart = (player) => this.savePlayer(player);
        const onTrackEnd = (player) => this.savePlayer(player);
        // queueEnd: the queue is empty + nothing playing. Persisting this idle
        // state (current: null, queue: []) means on restart the bot rejoins the
        // voice channel and sits idle — playerResumed fires but no audio plays.
        // Instead, remove the guild from the store so it's NOT restored.
        const onQueueEnd = (player) => this.removePlayer(player.guildId);
        // playerCreate: only save if there's something worth restoring (a
        // current track or a non-empty queue). A freshly created player with
        // no track + empty queue isn't worth persisting.
        const onPlayerCreate = (player) => {
            if (player.current || (player.queue && player.queue.length > 0)) {
                this.savePlayer(player);
            }
        };
        const onPlayerMove = (player) => this.savePlayer(player);
        // Hook playerDisconnect (emitted by Player.destroy()) instead of just
        // playerDestroy (only emitted by Riffy.destroyPlayer()). If a user or
        // internal code calls player.destroy() directly, playerDisconnect
        // still fires, so we clean up persisted state. Without this, a
        // directly-destroyed player's state stays in the store and gets
        // re-restored on the next restart — resurrecting a killed player.
        const onPlayerDisconnect = (player) => this.removePlayer(player.guildId);
        const onPlayerDestroy = (player) => this.removePlayer(player.guildId);
        const onPlayerUpdate = (player) => this.savePlayer(player);

        riffy.on("trackStart", onTrackStart);
        riffy.on("trackEnd", onTrackEnd);
        riffy.on("queueEnd", onQueueEnd);
        riffy.on("playerCreate", onPlayerCreate);
        riffy.on("playerMove", onPlayerMove);
        riffy.on("playerDisconnect", onPlayerDisconnect);
        riffy.on("playerDestroy", onPlayerDestroy);
        riffy.on("playerUpdate", onPlayerUpdate);

        this._riffyHandlers = [
            ["trackStart", onTrackStart], ["trackEnd", onTrackEnd],
            ["queueEnd", onQueueEnd], ["playerCreate", onPlayerCreate],
            ["playerMove", onPlayerMove], ["playerDisconnect", onPlayerDisconnect],
            ["playerDestroy", onPlayerDestroy], ["playerUpdate", onPlayerUpdate],
        ];

        // Best-effort final flush on process exit. Sync saveSync() is used
        // when available (JsonFileStorage, ShardedJsonStorage) because async
        // I/O on process.exit is unreliable (the event loop is torn down).
        //
        // For adapters that only have async save() (SqliteStorage, custom
        // Redis/Postgres), the SIGINT/SIGTERM handlers use a synchronous
        // flush pattern (saveSync if available) and the async save() calls
        // may not complete before the process exits. This is a known
        // limitation — for production use with async-only adapters, consider
        // implementing saveSync() or using a graceful shutdown pattern
        // (calling await resumeManager.save() + await storage.close() in
        // your own shutdown handler before process.exit()).
        const safeFlush = () => {
            try {
                for (const [guildId, timer] of this._saveTimers) {
                    clearTimeout(timer);
                    const state = this._state.players[guildId];
                    if (state) {
                        if (typeof this.storage.saveSync === "function") {
                            this.storage.saveSync(guildId, state);
                        } else {
                            // Async-only adapter — fire and forget. May not
                            // complete on unclean shutdown (documented above).
                            this.storage.save(guildId, state).catch(() => {});
                        }
                    }
                }
                this._saveTimers.clear();
                // Close the storage adapter to release resources (e.g. the
                // SQLite database handle). Safe to call even if close() is
                // a no-op (JsonFileStorage / ShardedJsonStorage).
                if (typeof this.storage.close === "function") {
                    this.storage.close().catch(() => {});
                }
            } catch (_) {
                /* ignore */
            }
        };

        const onExit = safeFlush;
        const onSIGINT = () => { safeFlush(); process.exit(0); };
        const onSIGTERM = () => { safeFlush(); process.exit(0); };

        process.on("exit", onExit);
        process.on("SIGINT", onSIGINT);
        process.on("SIGTERM", onSIGTERM);

        this._processHandlers = { onExit, onSIGINT, onSIGTERM };
    }

    /**
     * Remove all event listeners and process handlers attached by
     * {@link attachListeners}. Also closes the storage adapter (releasing
     * resources like the SQLite database handle) and clears all pending
     * per-guild save timers.
     *
     * Call this when replacing or disposing of a ResumeManager instance
     * (e.g. in tests, or if the user calls init() again with different
     * options). Without this, stale closures remain on the riffy
     * EventEmitter and on process, keeping the old instance alive and
     * causing duplicate handler invocations.
     *
     * @since 1.0.15
     */
    destroy() {
        // Remove riffy event listeners.
        if (this._riffyHandlers) {
            for (const [event, handler] of this._riffyHandlers) {
                this.riffy.off(event, handler);
            }
            this._riffyHandlers = null;
        }
        // Remove process signal handlers.
        if (this._processHandlers) {
            const { onExit, onSIGINT, onSIGTERM } = this._processHandlers;
            process.off("exit", onExit);
            process.off("SIGINT", onSIGINT);
            process.off("SIGTERM", onSIGTERM);
            this._processHandlers = null;
        }
        // Clear pending save timers.
        for (const timer of this._saveTimers.values()) clearTimeout(timer);
        this._saveTimers.clear();
        // Close the storage adapter (releases DB handles etc).
        if (this.storage && typeof this.storage.close === "function") {
            this.storage.close().catch(() => {});
        }
        this._listenersAttached = false;
    }

    /**
     * Clear all persisted state — both in-memory and the backing store.
     * Also removes process signal handlers (so stale handlers don't
     * accumulate if clear() is called before re-init). Useful for testing
     * or a manual reset.
     */
    async clear() {
        this._state = { version: 1, savedAt: 0, players: {} };
        for (const t of this._saveTimers.values()) clearTimeout(t);
        this._saveTimers.clear();
        // Remove process signal handlers to prevent accumulation.
        if (this._processHandlers) {
            const { onExit, onSIGINT, onSIGTERM } = this._processHandlers;
            process.off("exit", onExit);
            process.off("SIGINT", onSIGINT);
            process.off("SIGTERM", onSIGTERM);
            this._processHandlers = null;
        }
        await this.storage.clear().catch(() => {});
    }

    /**
     * Get a read-only snapshot of the current persisted state.
     */
    get snapshot() {
        return JSON.parse(JSON.stringify(this._state));
    }

    /**
     * Whether restoreAll() has completed AND clearOnRestore wiped the store.
     * Useful for callers to check if a full restore+clear cycle happened
     * (e.g. to decide whether to re-arm auto-restore after a manual
     * load() + restoreAll() cycle).
     * @since 1.0.15
     */
    get isFullyRestored() {
        return this._restoredFully;
    }
}

module.exports = { ResumeManager };
