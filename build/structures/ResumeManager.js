const fs = require("node:fs");
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

        /** @type {{ version: number, savedAt: number, players: Record<string, any> }} */
        this._state = { version: 1, savedAt: 0, players: {} };
        this._restored = false;
        this._restoredFully = false;
        this._listenersAttached = false;
        /** @type {Map<string, (reason: string, detail?: any) => void>} guildId -> reject fn for active restores */
        this._pendingRestores = new Map();
        /** @type {Map<string, NodeJS.Timeout>} guildId -> per-guild debounce timer */
        this._saveTimers = new Map();
        /** @type {Promise<Map<string, any>> | null} */
        this._loadPromise = null;

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
        if (typeof state.voiceChannel !== "string" || !state.voiceChannel) return { ok: false, reason: "missing voiceChannel" };
        if (state.volume !== undefined && typeof state.volume !== "number") return { ok: false, reason: "volume is not a number" };
        if (state.position !== undefined && typeof state.position !== "number") return { ok: false, reason: "position is not a number" };
        if (state.paused !== undefined && typeof state.paused !== "boolean") return { ok: false, reason: "paused is not a boolean" };
        if (state.queue !== undefined && !Array.isArray(state.queue)) return { ok: false, reason: "queue is not an array" };
        if (state.current !== undefined && state.current !== null && typeof state.current !== "object") return { ok: false, reason: "current is not an object" };
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
     * @private
     */
    _safeSerializeInfo(info) {
        if (!info) return null;
        const out = {
            identifier: info.identifier ?? null,
            isSeekable: info.seekable ?? false,
            author: info.author ?? null,
            length: info.length ?? 0,
            isStream: info.stream ?? false,
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
     */
    async _rebuildTrack(data, node) {
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
                this.riffy.emit("debug", `[ResumeManager] requesterResolver threw for guild: ${e.message}`);
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
        if (this._restored) return [];
        this._restored = true;

        // Await any pending load (the adapter may still be reading).
        if (this._loadPromise) {
            try { await this._loadPromise; } catch (_) { /* already logged */ }
        }

        const guildIds = Object.keys(this._state.players || {});
        if (!guildIds.length) {
            this.riffy.emit("debug", `[ResumeManager] No saved players to restore.`);
            if (this.clearOnRestore) { await this.storage.clear().catch(() => {}); }
            return [];
        }

        this.riffy.emit("debug", `[ResumeManager] Restoring ${guildIds.length} player(s)...`);
        const restored = [];

        for (const guildId of guildIds) {
            const state = this._state.players[guildId];
            try {
                const player = await this.restorePlayer(state);
                if (player) restored.push(player);
            } catch (e) {
                this.riffy.emit("debug", `[ResumeManager] Failed to restore player ${guildId}: ${e.message}`);
                delete this._state.players[guildId];
            }
        }

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
        const restorePromise = new Promise(async (resolve, reject) => {
            // Register the reject so socketClosed / timeout can abort.
            // Setting `aborted = true` first ensures the executor checks it.
            this._pendingRestores.set(guildId, (reason, detail) => {
                aborted = true;
                reject({ reason, detail });
            });

            try {
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

                // 3. Rebuild the queue. Bail out if aborted mid-rebuild.
                if (Array.isArray(state.queue) && !isAborted()) {
                    for (const t of state.queue) {
                        const rebuilt = await this._rebuildTrack(t, node);
                        if (rebuilt) player.queue.add(rebuilt);
                        if (isAborted()) break;
                    }
                }

                // 4. Restore current track + seek to the saved position.
                if (!isAborted() && state.current && (state.current.encoded || (state.current.info && state.current.info.identifier))) {
                    const currentTrack = await this._rebuildTrack(state.current, node);
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
                    // the error MUST propagate so restorePlayer's catch block
                    // calls _failRestore → destroys the player, removes it from
                    // state, and emits playerRestoreFailed. Previously this catch
                    // swallowed the error, leaving an unusable player registered
                    // with no failure event (review: "Queue restore reports
                    // false success").
                    await player.play();
                }

                // If the restore was aborted while we were waiting, do NOT
                // resolve — the promise has already been rejected, and the
                // caller's catch block runs _failRestore. Resolving here would
                // be a no-op (promise already settled), but we also must not
                // emit the "Restored player" debug line.
                if (isAborted()) {
                    return;
                }

                this.riffy.emit(
                    "debug",
                    `[ResumeManager] Restored player for guild ${guildId} (voice=${state.voiceChannel}, queue=${player.queue.length}, position=${state.position || 0}ms, paused=${state.paused ?? false})`
                );
                resolve(player);
            } catch (err) {
                // If aborted, prefer the abort reason over this error.
                if (!isAborted()) reject({ reason: "error", detail: err });
            }
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
                player.destroy();
            } catch (e) {
                this.riffy.emit("debug", `[ResumeManager] Error destroying failed player for ${guildId}: ${e.message}`);
            }
            // Safety net: always remove from the players map, even if
            // destroy() threw partway through and never reached the
            // internal `this.riffy.players.delete(guildId)` line.
            this.riffy.players.delete(guildId);
        }

        // Remove from persisted state so it isn't retried on every restart.
        if (this._state.players[guildId]) {
            delete this._state.players[guildId];
            // Persist the cleanup immediately (unless clearOnRestore will wipe all).
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
            this.riffy.emit("debug", `[ResumeManager] No encoded track to resume for ${player.guildId}`);
            return;
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
        // orphan Lavalink player. Send a DELETE to clean it up — BUT only if
        // the player instance we PATCHed is still the one registered for
        // this guild. If _failRestore emitted playerRestoreFailed and the
        // user's handler created a REPLACEMENT player, this.players.get()
        // now points at a different instance; a guild-scoped DELETE would
        // kill the replacement's Lavalink session (review: "replacement
        // player's Lavalink session silently stopped").
        if (isAborted()) {
            const currentPlayer = this.riffy.players.get(player.guildId);
            if (currentPlayer === player) {
                // No replacement — safe to delete the orphan we may have
                // just recreated on Lavalink.
                this.riffy.emit("debug", `[ResumeManager] Restore aborted during/after PATCH for ${player.guildId}; sending DELETE to clean up orphan Lavalink player.`);
                try {
                    await player.node.rest.destroyPlayer(player.guildId);
                } catch (e) {
                    this.riffy.emit("debug", `[ResumeManager] Orphan-cleanup DELETE failed for ${player.guildId}: ${e.message}`);
                }
            } else {
                // A replacement player exists for this guild — do NOT send
                // a guild-scoped DELETE, it would kill the replacement.
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
     */
    attachListeners() {
        if (!this.enabled || this._listenersAttached) return;
        this._listenersAttached = true;

        const riffy = this.riffy;

        riffy.on("trackStart", (player) => this.savePlayer(player));
        riffy.on("trackEnd", (player) => this.savePlayer(player));
        riffy.on("queueEnd", (player) => this.savePlayer(player));
        riffy.on("playerCreate", (player) => this.savePlayer(player));
        riffy.on("playerMove", (player) => this.savePlayer(player));
        riffy.on("playerDestroy", (player) => this.removePlayer(player.guildId));

        // playerUpdate fires frequently (~every 5s) from Lavalink with the
        // current position. Throttled via saveInterval (per-guild).
        riffy.on("playerUpdate", (player) => this.savePlayer(player));

        // Best-effort final flush on process exit. Flushes all pending
        // per-guild debounced writes synchronously (the adapters use sync fs
        // writes under the hood, so this works on the exit tick).
        const safeFlush = () => {
            try {
                for (const [guildId, timer] of this._saveTimers) {
                    clearTimeout(timer);
                    const state = this._state.players[guildId];
                    if (state) this.storage.save(guildId, state).catch(() => {});
                }
                this._saveTimers.clear();
            } catch (_) {
                /* ignore */
            }
        };
        process.on("exit", safeFlush);
        process.on("SIGINT", () => {
            safeFlush();
            process.exit(0);
        });
        process.on("SIGTERM", () => {
            safeFlush();
            process.exit(0);
        });
    }

    /**
     * Clear all persisted state — both in-memory and the backing store.
     * Useful for testing or a manual reset.
     */
    async clear() {
        this._state = { version: 1, savedAt: 0, players: {} };
        for (const t of this._saveTimers.values()) clearTimeout(t);
        this._saveTimers.clear();
        await this.storage.clear().catch(() => {});
    }

    /**
     * Get a read-only snapshot of the current persisted state.
     */
    get snapshot() {
        return JSON.parse(JSON.stringify(this._state));
    }
}

module.exports = { ResumeManager };
