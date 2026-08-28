# feat: client-restart auto-resume with disk persistence

## Summary

Adds a new **client-restart auto-resume** feature to Riffy. When the bot process crashes or restarts, Riffy now automatically **rejoins the same voice channel**, **replays the same song**, **seeks to the exact saved position**, and **restores the full queue** — all from a JSON state file persisted to disk.

This is distinct from the existing node-level `autoResume` (which only handles Lavalink WebSocket reconnects within a single process lifetime). This PR survives a **full process restart**.

Also fixes a pre-existing bug: `Node.open()` calls `player.restart()` when `autoResume` is enabled, but `Player.restart()` **did not exist** — causing a `TypeError` on every Lavalink WebSocket reconnect. This PR implements it.

---

## Motivation

When a Discord music bot restarts (deploy, crash, autoscaler, etc.), every player is lost. Users have to manually re-invite the bot, re-queue their songs, and lose their playback position. This is a poor UX that every production Riffy bot currently has to work around with custom (and usually buggy) persistence code.

This PR bakes that capability into Riffy itself, opt-in via a single `resume` option.

---

## Changes

### New file
- **`build/structures/ResumeManager.js`** — the persistence + restore orchestrator (644 lines)

### Modified files
- **`build/structures/Player.js`** — implements the missing `restart()` method (+67 lines)
- **`build/structures/Riffy.js`** — wires the `resume` option into `init()`, adds `resumePlayers()` (+45 lines)
- **`build/index.js`** — exports `ResumeManager`
- **`build/index.d.ts`** — full type definitions (+281 lines)

**No breaking changes.** The `resume` option is strictly opt-in. When omitted, behavior is identical to before.

---

## Usage

```js
const { Riffy } = require("riffy");

client.riffy = new Riffy(client, nodes, {
  send: (payload) => { /* ... */ },
  restVersion: "v4",

  // ── NEW: client-restart auto-resume ──
  resume: {
    enabled: true,
    filePath: "./data/riffy-state.json", // where state is persisted
    saveInterval: 3000,                  // debounce window for disk writes (ms)
    clearOnRestore: false,               // delete state file after successful restore
    restoreTimeout: 15000,               // abort restore if voice creds don't arrive (ms)
    requesterResolver: (id) => client.users.fetch(id).catch(() => id),
  },
});

client.on("ready", () => {
  // init() loads persisted state + arms auto-restore on first nodeConnect.
  client.riffy.init(client.user.id);
});

// Fires for every player restored after a client restart:
client.riffy.on("playerResumed", (player) => {
  const ch = client.channels.cache.get(player.textChannel);
  ch?.send(`↩️ Resumed "${player.current?.info.title}" at ${player.position}ms`);
});

// Fires when a restore couldn't complete (channel deleted / bot kicked / no permission):
client.riffy.on("playerRestoreFailed", (guildId, reason, detail, state) => {
  console.error(`Restore failed for ${guildId}: ${reason} (${detail})`);
  if (state?.textChannel) {
    client.channels.fetch(state.textChannel).then(c => c?.send(
      "⚠️ I couldn't rejoin the voice channel after restarting — it may have been deleted or I'm missing the Connect permission."
    )).catch(() => {});
  }
});
```

### What gets restored?

| Field | Restored? | Notes |
|-------|-----------|-------|
| Voice channel | ✅ | Bot rejoins the same voice channel |
| Current track | ✅ | Re-sent to Lavalink at the exact saved position (seek) |
| Playback position | ✅ | Seeked to the millisecond |
| **Paused state** | ✅ | If the player was paused before the restart, it stays paused — the bot doesn't surprise users by suddenly playing audio they paused |
| Volume | ✅ | Restored to the saved value |
| Loop mode | ✅ | `none`, `track`, or `queue` |
| Queue | ✅ | Rebuilt from persisted encoded track strings (capped by `maxQueueSize`) |
| Deaf/Mute | ✅ | Self-deaf/self-mute state restored |

The `paused` state is saved as part of `serializePlayer()` and sent to Lavalink via `updatePlayer({ paused: state.paused })` during restore. So a paused player rejoins paused — it will NOT auto-play unless the user explicitly resumes it.

---

## New API

### `ResumeOptions`

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | `boolean` | `false` | Master switch. |
| `filePath` | `string` | `<cwd>/riffy-state.json` | Where the JSON state file is written/read. |
| `saveInterval` | `number` | `3000` | Debounce window (ms) for disk writes. |
| `clearOnRestore` | `boolean` | `false` | Delete the state file + wipe in-memory state once `restoreAll()` finishes. Prevents stale state re-applying on every restart. |
| `restoreTimeout` | `number` | `15000` | Max ms to wait for voice credentials per player before aborting. |
| `requesterResolver` | `(id) => any` | `—` | Rebuild a requester object from its persisted id. |

### New events

- **`playerResumed`** `(player: Player)` — fires on successful restore.
- **`playerRestoreFailed`** `(guildId: string, reason: RestoreFailReason, detail: string, state: SerializedPlayer | null)` — fires when a restore aborts.

### `RestoreFailReason`

`"timeout" | "socket_closed" | "no_nodes" | "invalid_state" | "error"`

### New `ResumeManager` class

Public methods: `load()`, `save()`, `serializePlayer()`, `savePlayer()`, `removePlayer()`, `restoreAll()`, `restorePlayer()`, `attachListeners()`, `destroy()`, `clear()`, `snapshot` getter, `isFullyRestored` getter.

### New `Player.restart()`

Rejoins the configured voice channel and resumes the current track at the last known position. Powers `Node.autoResume` (which was previously broken) and is reused by `ResumeManager`.

### New `Riffy` members

- `riffy.resumeManager` — the active `ResumeManager`, or `null` if resume is disabled.
- `riffy.resumePlayers()` — manually trigger `restoreAll()`.

---

## How it works

### Save path (while the bot runs)
`ResumeManager` listens to `trackStart`, `trackEnd`, `queueEnd`, `playerCreate`, `playerMove`, `playerDestroy`, and `playerUpdate` events. On each, it serializes the player to a JSON-safe object (dropping the non-serializable `requester`, keeping its id) and debounces a write to disk (`saveInterval`). On `process.exit` / `SIGINT` / `SIGTERM` it does one final sync flush.

### Restore path (after a restart)
1. `init()` → `ResumeManager.load()` reads the JSON file into memory (validating each entry).
2. On the first `nodeConnect`, `restoreAll()` runs after a 1s grace.
3. For each saved guild:
   - `createConnection({ guildId, voiceChannel })` → re-sends `VOICE_STATE_UPDATE` to rejoin.
   - Rebuilds the queue from persisted encoded track strings.
   - Waits for `connection.resolve()` (Discord voice credentials).
   - `node.rest.updatePlayer({ track, position, volume, paused })` → seeks to the exact saved position.
   - Emits `playerResumed`.

---

## Failure handling

This PR explicitly handles the edge cases raised by maintainers:

| Scenario | Behavior |
|----------|----------|
| **Voice channel deleted while bot offline** | `restoreTimeout` (15s default) aborts the restore. Half-created player is destroyed. Guild is removed from persisted state. `playerRestoreFailed` with `reason="timeout"` fires. |
| **Bot kicked from guild** | Same as above. |
| **Bot lacks Connect permission** | Same as above, or aborts early on a fatal `WebSocketClosedEvent` (codes 4006/4009/4014/4015) with `reason="socket_closed"`. |
| **Corrupt state file** (partial write during crash) | `load()` moves the file aside to `<file>.corrupt-<timestamp>` and returns `false`. No crash. |
| **Malformed entries** (missing `guildId`/`voiceChannel`, wrong types) | Dropped + logged. Valid entries are kept. Cleaned state is re-persisted. |
| **No connected Lavalink nodes** | `playerRestoreFailed` with `reason="no_nodes"`. |

The `playerRestoreFailed` event includes the persisted `state` (which has `textChannel`), so the bot can notify the user that it couldn't rejoin.

---

## Testing

All scenarios were verified with integration tests in Node:

- ✅ Save → crash → reload → restore round-trip (position + queue preserved to the millisecond).
- ✅ `clearOnRestore: true` deletes the file + wipes memory after restore; second `restoreAll()` is a no-op.
- ✅ `clearOnRestore: false` keeps the file as a rolling snapshot.
- ✅ `restoreTimeout` aborts on missing voice credentials (channel deleted).
- ✅ `socketClosed` aborts early on fatal codes (4006/4009/4014/4015).
- ✅ Half-created player is destroyed on failure (safety-net `players.delete()` even if `destroy()` throws).
- ✅ Corrupt file → moved aside, `load()` returns `false`.
- ✅ Malformed entries → dropped, valid ones kept.
- ✅ `no_nodes` → `playerRestoreFailed` with `reason="no_nodes"`.
- ✅ All files pass `node --check`.
- ✅ **Storage adapters:** JsonFileStorage, ShardedJsonStorage, SqliteStorage, + custom adapter — all pass integration tests (save/load/remove/clear, per-guild debounce, maxQueueSize pruning, backward compat).

---

## Scaling (addresses maintainer feedback on large player counts)

A single JSON file doesn't scale — a bot in thousands of guilds produces one massive file with **write amplification** (every `playerUpdate` re-serializes ALL players) and a crash mid-write corrupts everything.

This PR ships a **pluggable storage adapter system** (commit 3). Pick a backend via the `storage` option:

| Preset | Adapter | Scales to | Notes |
|--------|---------|-----------|-------|
| `"json"` | `JsonFileStorage` | <~100 players | Default; backward compat. Single file. |
| `"sharded"` | `ShardedJsonStorage` | thousands | One file per guild. No amplification, crash isolation per guild. **Recommended for most production bots.** |
| `"sqlite"` | `SqliteStorage` | tens of thousands | Indexed DB, ACID. Needs `better-sqlite3` (optional peer dep, lazy-loaded). |
| custom | `StorageAdapter` subclass | unlimited | Implement `init`/`loadAll`/`save`/`remove`/`clear`/`close` for Redis, PostgreSQL, MongoDB, S3, etc. |

Plus two scaling-oriented improvements:
- **Per-guild debounce** — each guild has its own save timer, so a busy guild never delays writes for other guilds.
- **`maxQueueSize`** option — caps per-guild queue size to prevent a single huge playlist from bloating storage.

```js
// Production bot with thousands of guilds:
new Riffy(client, nodes, {
  send, restVersion: "v4",
  resume: {
    enabled: true,
    storage: "sharded",                // one file per guild
    filePath: "./data/riffy-state",    // directory
    saveInterval: 3000,
    maxQueueSize: 100,                 // cap per-guild queue
  },
});
```

No breaking changes — omitting `storage` defaults to `JsonFileStorage` using `filePath`, identical to v1.0.14 behavior.

---

## Checklist

- [x] Code follows the existing style (CommonJS, JSDoc, conventional commits)
- [x] No breaking changes — `resume` is strictly opt-in
- [x] Full TypeScript definitions added to `index.d.ts`
- [x] All new code documented with JSDoc
- [x] Edge cases handled (channel deleted, permission denied, corrupt state, no nodes)
- [x] `node --check` passes on all modified files
- [x] Integration tests pass for save/restore + all failure paths
- [x] Existing `Node.autoResume` bug (missing `Player.restart()`) fixed as a prerequisite

---

## Related

- Fixes the latent `Player.restart()` bug referenced by `Node.open()` (line 630 of `Node.js`).
- Supersedes any custom persistence workarounds bot authors currently maintain.

---

**Note to maintainers:** This is a large additive feature (~1040 lines, 5 files). I'm happy to split it into smaller PRs if preferred — e.g. (1) the `Player.restart()` fix alone, (2) the `ResumeManager` + wiring. Just let me know.
