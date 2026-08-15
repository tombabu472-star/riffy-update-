const fs = require("node:fs");
const path = require("node:path");
const { Track } = require("./Track");

/**
 * ResumeManager
 *
 * Provides client-restart auto-resume for Riffy. Persists player state
 * (voice channel, text channel, current track, playback position, volume,
 * loop mode, paused state, and the full queue) to a JSON file on disk and
 * restores all players after the bot process restarts.
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
 */
class ResumeManager {
    /**
     * @param {import("./Riffy").Riffy} riffy
     * @param {Object} [options]
     * @param {boolean} [options.enabled=false] Enable client-restart resume.
     * @param {string} [options.filePath] Path to the state JSON file. Defaults to `<cwd>/riffy-state.json`.
     * @param {number} [options.saveInterval=3000] Debounce window (ms) for disk writes.
     * @param {(requester: any) => any} [options.requesterResolver] Optional function to rebuild a requester object from the persisted value.
     */
    constructor(riffy, options = {}) {
        this.riffy = riffy;
        this.enabled = options.enabled ?? false;
        this.filePath = options.filePath || path.join(process.cwd(), "riffy-state.json");
        this.saveInterval = typeof options.saveInterval === "number" ? options.saveInterval : 3000;
        this.requesterResolver = typeof options.requesterResolver === "function" ? options.requesterResolver : null;
        /**
         * When `true`, the persisted state file is deleted from disk once a
         * full restore completes successfully. This prevents stale state from
         * accumulating and being re-applied on every subsequent restart.
         *
         * The in-memory state is also wiped, so a second restoreAll() call
         * becomes a no-op (returns `[]`). New player activity after the
         * restore will be persisted fresh as usual.
         *
         * Default: `false` (state is kept across restarts).
         */
        this.clearOnRestore = options.clearOnRestore ?? false;
        /**
         * Maximum time (ms) to wait for voice credentials when restoring a
         * single player. If Discord doesn't reply with a VOICE_SERVER_UPDATE
         * in time (e.g. the voice channel was deleted, the bot was kicked,
         * or it lacks Connect permission), the restore for that guild is
         * aborted, the half-created player is destroyed, the guild is
         * removed from persisted state, and a `playerRestoreFailed` event
         * is emitted with reason `"timeout"`.
         *
         * Default: `15000` (15 seconds).
         */
        this.restoreTimeout = typeof options.restoreTimeout === "number" ? options.restoreTimeout : 15000;

        /** @type {{ version: number, savedAt: number, players: Record<string, any> }} */
        this._state = { version: 1, savedAt: 0, players: {} };
        this._saveTimer = null;
        this._dirty = false;
        this._restored = false;
        this._restoredFully = false;
        this._listenersAttached = false;
        /** @type {Map<string, (reason: string, detail?: any) => void>} guildId -> reject fn for active restores */
        this._pendingRestores = new Map();
    }

    /**
     * Load persisted state from disk into memory. Corrupt or partial writes
     * (e.g. from a crash mid-flush or storage loss) are handled gracefully:
     * if the file can't be parsed, or individual player entries are missing
     * required fields, those entries are dropped and logged rather than
     * crashing the restore.
     * @returns {boolean} Whether any valid players were loaded.
     */
    load() {
        if (!this.enabled) return false;
        try {
            if (!fs.existsSync(this.filePath)) return false;
            const raw = fs.readFileSync(this.filePath, "utf-8");
            const parsed = JSON.parse(raw);
            if (!parsed || typeof parsed !== "object" || !parsed.players) return false;

            const validPlayers = {};
            let dropped = 0;
            for (const [guildId, state] of Object.entries(parsed.players)) {
                const validation = this._validatePlayerState(guildId, state);
                if (validation.ok) {
                    validPlayers[guildId] = state;
                } else {
                    dropped++;
                    this.riffy.emit("debug", `[ResumeManager] Dropped invalid persisted entry for guild ${guildId}: ${validation.reason}`);
                }
            }

            this._state = { version: 1, savedAt: parsed.savedAt || 0, players: validPlayers };
            const count = Object.keys(validPlayers).length;
            this.riffy.emit("debug", `[ResumeManager] Loaded state for ${count} player(s) from ${this.filePath}${dropped ? ` (dropped ${dropped} invalid entry/entries)` : ""}`);

            // If we dropped invalid entries, persist the cleaned-up state.
            if (dropped > 0) this.save(true);
            return count > 0;
        } catch (e) {
            this.riffy.emit("debug", `[ResumeManager] Failed to load state from ${this.filePath} (likely corrupt or partial write): ${e.message}`);
            // Move the corrupt file aside so future saves aren't blocked by it.
            try {
                if (fs.existsSync(this.filePath)) {
                    const backup = this.filePath + ".corrupt-" + Date.now();
                    fs.renameSync(this.filePath, backup);
                    this.riffy.emit("debug", `[ResumeManager] Moved corrupt state file to ${backup}`);
                }
            } catch (_) {
                /* ignore */
            }
            return false;
        }
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
     * Schedule a debounced write to disk.
     * @param {boolean} [immediate=false] If true, flush synchronously right now.
     */
    save(immediate = false) {
        if (!this.enabled) return;
        this._dirty = true;
        if (immediate) {
            this._flush();
            return;
        }
        if (this._saveTimer) return;
        this._saveTimer = setTimeout(() => {
            this._saveTimer = null;
            this._flush();
        }, this.saveInterval);
    }

    /** @private */
    _flush() {
        if (!this._dirty) return;
        this._dirty = false;
        try {
            this._state.savedAt = Date.now();
            const dir = path.dirname(this.filePath);
            if (dir && !fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
            fs.writeFileSync(this.filePath, JSON.stringify(this._state, null, 2), "utf-8");
        } catch (e) {
            this.riffy.emit("debug", `[ResumeManager] Failed to save state to ${this.filePath}: ${e.message}`);
        }
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
     * Serialize a player into a JSON-safe object.
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

        const queue = (player.queue || []).map((t) => ({
            encoded: t.track || t.encoded || null,
            info: this._safeSerializeInfo(t.info),
            pluginInfo: t.pluginInfo || null,
        }));

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
     * Update stored state for a single player and schedule a save.
     * @param {import("./Player").Player} player
     */
    savePlayer(player) {
        if (!this.enabled) return;
        const serialized = this.serializePlayer(player);
        if (!serialized) return;
        this._state.players[player.guildId] = serialized;
        this.save();
    }

    /**
     * Remove a player from persisted state (e.g. when destroyed).
     * @param {string} guildId
     */
    removePlayer(guildId) {
        if (!this.enabled) return;
        if (this._state.players[guildId]) {
            delete this._state.players[guildId];
            this.save();
        }
    }

    /**
     * Reconstruct a Track object from serialized data.
     * @private
     */
    _rebuildTrack(data, node) {
        if (!data) return null;
        const encoded = data.encoded || null;
        let requester = data.info?.requester ?? null;
        if (requester !== null && this.requesterResolver) {
            try {
                requester = this.requesterResolver(requester) ?? requester;
            } catch (_) {
                /* keep primitive requester */
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

        const guildIds = Object.keys(this._state.players || {});
        if (!guildIds.length) {
            this.riffy.emit("debug", `[ResumeManager] No saved players to restore.`);
            // Still honor clearOnRestore so a leftover empty/stale file is removed.
            if (this.clearOnRestore) this._clearDiskFile();
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
            // Wipe in-memory state and delete the on-disk file so it isn't
            // re-applied on the next restart. Fresh state will be written as
            // new player activity happens.
            this._state = { version: 1, savedAt: 0, players: {} };
            this._dirty = false;
            if (this._saveTimer) { clearTimeout(this._saveTimer); this._saveTimer = null; }
            this._clearDiskFile();
            this._restoredFully = true;
            this.riffy.emit("debug", `[ResumeManager] clearOnRestore enabled — state file removed, in-memory state wiped.`);
        } else {
            this.save(true);
        }

        return restored;
    }

    /**
     * Delete the persisted state file from disk (if it exists).
     * @private
     */
    _clearDiskFile() {
        try {
            if (fs.existsSync(this.filePath)) {
                fs.unlinkSync(this.filePath);
                this.riffy.emit("debug", `[ResumeManager] Deleted state file: ${this.filePath}`);
            }
        } catch (e) {
            this.riffy.emit("debug", `[ResumeManager] Failed to delete state file ${this.filePath}: ${e.message}`);
        }
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
        const restorePromise = new Promise(async (resolve, reject) => {
            // Register the reject so socketClosed / timeout can abort.
            this._pendingRestores.set(guildId, (reason, detail) => reject({ reason, detail }));

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

                // 3. Rebuild the queue.
                if (Array.isArray(state.queue)) {
                    for (const t of state.queue) {
                        const rebuilt = this._rebuildTrack(t, node);
                        if (rebuilt) player.queue.add(rebuilt);
                    }
                }

                // 4. Restore current track + seek to the saved position.
                if (state.current && (state.current.encoded || (state.current.info && state.current.info.identifier))) {
                    const currentTrack = this._rebuildTrack(state.current, node);
                    player.current = currentTrack;
                    player.position = state.position || 0;
                    player.paused = state.paused ?? false;

                    // Send the resume payload once voice credentials arrive.
                    await this._sendResumePayload(player, state);
                } else if (player.queue.length > 0) {
                    try {
                        await player.play();
                    } catch (e) {
                        this.riffy.emit("debug", `[ResumeManager] Auto-play after restore failed for ${guildId}: ${e.message}`);
                    }
                }

                this.riffy.emit(
                    "debug",
                    `[ResumeManager] Restored player for guild ${guildId} (voice=${state.voiceChannel}, queue=${player.queue.length}, position=${state.position || 0}ms, paused=${state.paused ?? false})`
                );
                resolve(player);
            } catch (err) {
                reject({ reason: "error", detail: err });
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
     * @private
     */
    async _failRestore(guildId, reason, detail, state) {
        if (!guildId) return;

        // Destroy the half-created player if it exists.
        const player = this.riffy.players.get(guildId);
        if (player) {
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
            // Persist the cleanup (unless clearOnRestore will handle it).
            if (!this.clearOnRestore) this.save(true);
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
     * @private
     */
    async _sendResumePayload(player, state) {
        try {
            await player.connection.resolve();
        } catch (e) {
            this.riffy.emit("debug", `[ResumeManager] Voice connection not ready for ${player.guildId}; attempting payload anyway: ${e.message}`);
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
        // current position. Throttled via saveInterval.
        riffy.on("playerUpdate", (player) => this.savePlayer(player));

        // Best-effort final flush on process exit (sync write).
        const safeFlush = () => {
            try {
                this._flush();
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
     * Clear all persisted state — both in-memory and the on-disk file.
     * Useful for testing or a manual reset.
     */
    clear() {
        this._state = { version: 1, savedAt: 0, players: {} };
        this._dirty = false;
        if (this._saveTimer) { clearTimeout(this._saveTimer); this._saveTimer = null; }
        this._clearDiskFile();
    }

    /**
     * Get a read-only snapshot of the current persisted state.
     */
    get snapshot() {
        return JSON.parse(JSON.stringify(this._state));
    }
}

module.exports = { ResumeManager };
