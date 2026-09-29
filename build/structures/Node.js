const Websocket = require("ws");
const { Rest } = require("./Rest");
const { Track } = require("./Track");

class Node {
  /**
   * @param {import("./Riffy").Riffy} riffy
   * @param {import("..").RiffyOptions} options
   * @param {import("..").LavalinkNode} node
   */
  constructor(riffy, node, options) {
    this.riffy = riffy
    this.name = node.name || node.host;
    this.host = node.host || "localhost";
    this.port = node.port || 2333;
    this.password = node.password || "youshallnotpass";
    this.restVersion = options.restVersion;
    this.secure = node.secure || false;
    this.sessionId = node.sessionId || null;
    this.rest = new Rest(riffy, this);
    Object.defineProperty(this, "options", {
      get() {
        return options
      }
    })

    if (options.restVersion === "v4") {
      this.wsUrl = `ws${this.secure ? "s" : ""}://${this.host}:${this.port}/v4/websocket`;
    } else {
      this.wsUrl = `ws${this.secure ? "s" : ""}://${this.host}:${this.port}`;
    }

    this.restUrl = `http${this.secure ? "s" : ""}://${this.host}:${this.port}`;
    this.ws = null;
    this.regions = node.regions;
    /**
     * Lavalink Info fetched While/After connecting.
     * @type {import("..").NodeInfo | null}
     */
    this.info = null;
    this.stats = {
      players: 0,
      playingPlayers: 0,
      uptime: 0,
      memory: {
        free: 0,
        used: 0,
        allocated: 0,
        reservable: 0,
      },
      cpu: {
        cores: 0,
        systemLoad: 0,
        lavalinkLoad: 0,
      },
      frameStats: {
        sent: 0,
        nulled: 0,
        deficit: 0,
      },
      detailedStats: null,
      /**
       * Nodelink specific stats
       */
      eventLoopLagP50: 0,
      /**
       * Nodelink specific stats
       */
      eventLoopLagP95: 0,
      /**
       * Nodelink specific stats
       */
      eventLoopLagP99: 0,
      /**
       * Nodelink specific stats
       */
      stuckRecoveries: 0,
    };

    this.connected = false;
    this._ready = false;
    this._destroyed = false;
    this._migrating = false;
    this._closePromise = null;

    this.resumeKey = options.resumeKey || null;
    this.resumeTimeout = options.resumeTimeout || 60;
    this.autoResume = options.autoResume || false;

    this.reconnectTimeout = options.reconnectTimeout || 5000;
    this.reconnectTries = options.reconnectTries || 3;
    this.reconnectAttempt = null;
    this.reconnectAttempted = 1;

    this.lastStats = Date.now();
  }


  lyrics = {
    /**
     * Checks if the node has all the required plugins available.
     * @param {boolean} [eitherOne=true] If set to true, will return true if at least one of the plugins is present.
     * @param {...string} plugins The plugins to look for.
     * @returns {Promise<boolean>} If the plugins are available.
     * @throws {RangeError} If the plugins are missing and node is disconnected..
     */
    checkAvailable: async (eitherOne = true, ...plugins) => {
      if (!this.sessionId || !this.connected) throw new Error(`Node (${this.name}) is not Ready/Connected.`)
      if (!plugins.length) plugins = ["lavalyrics-plugin", "java-lyrics-plugin", "lyrics"];

      const missingPlugins = [];

      plugins.forEach((plugin) => {
        const p = this.info?.plugins?.find((p) => p.name === plugin)

        if (!p) {
          missingPlugins.push(plugin)
          return false;
        }

        return true;
      });

      const AllPluginsMissing = missingPlugins.length === plugins.length;

      if (eitherOne && AllPluginsMissing) {
        throw new RangeError(`Node (${this.name}) is missing plugins: ${missingPlugins.join(", ")} (required for Lyrics)`)
      } else if (!eitherOne && missingPlugins.length) {
        throw new RangeError(`Node (${this.name}) is missing plugins: ${missingPlugins.join(", ")} (required for Lyrics)`)
      }

      return true
    },

    /**
     * Fetches lyrics for a given track or encoded track string.
     *
     * @param {Track|string} trackOrEncodedTrackStr - The track object or encoded track string.
     * @param {boolean} [skipTrackSource=false] - Whether to skip the track source and fetch from the highest priority source (configured on Lavalink Server).
     * @returns {Promise<Object|null>} The lyrics data or null if the plugin is unavailable Or If no lyrics were found OR some Http request error occured.
     * @throws {TypeError} If `trackOrEncodedTrackStr` is not a `Track` or `string`.
     */
    get: async (trackOrEncodedTrackStr, skipTrackSource = false) => {
      if (!(await this.lyrics.checkAvailable(false, "lavalyrics-plugin"))) return null;
      if (!(trackOrEncodedTrackStr instanceof Track) && typeof trackOrEncodedTrackStr !== "string") throw new TypeError(`Expected \`Track\` or \`string\` for \`trackOrEncodedTrackStr\` in "lyrics.get" but got \`${typeof trackOrEncodedTrackStr}\``)

      let encodedTrackStr = typeof trackOrEncodedTrackStr === "string" ? trackOrEncodedTrackStr : trackOrEncodedTrackStr.track;

      return await this.rest.makeRequest("GET", `/v4/lyrics?skipTrackSource=${skipTrackSource}&track=${encodedTrackStr}`);
    },

    /** @description fetches Lyrics for Currently playing Track
     * @param {string} guildId The Guild Id of the Player
     * @param {boolean} [skipTrackSource=false] skips the Track Source & fetches from highest priority source (configured on Lavalink Server)
     * @param {string} [plugin] The Plugin to use(**Only required if you have too many known (i.e java-lyrics-plugin, lavalyrics-plugin) Lyric Plugins**)
     */
    getCurrentTrack: async (guildId, skipTrackSource = false, plugin = "") => {
      const DEFAULT_PLUGIN = "lavalyrics-plugin"
      if (!(await this.lyrics.checkAvailable())) return null;

      const nodePlugins = this.info?.plugins;
      let requestURL = `/v4/sessions/${this.sessionId}/players/${guildId}/track/lyrics?skipTrackSource=${skipTrackSource}&plugin=${plugin}`

      // If no `plugin` param is specified, check for `java-lyrics-plugin` or `lyrics` (also if lavalyrics-plugin is not available)
      if (!plugin && (nodePlugins.find((p) => p.name === "java-lyrics-plugin") || nodePlugins.find((p) => p.name === "lyrics")) && !(nodePlugins.find((p) => p.name === DEFAULT_PLUGIN))) {
        requestURL = `/v4/sessions/${this.sessionId}/players/${guildId}/lyrics?skipTrackSource=${skipTrackSource}`
      } else if (plugin && ["java-lyrics-plugin", "lyrics"].includes(plugin)) {
        // If `plugin` param is specified, And it's one of either `lyrics` or `java-lyrics-plugin`
        requestURL = `/v4/sessions/${this.sessionId}/players/${guildId}/lyrics?skipTrackSource=${skipTrackSource}`
      }

      return await this.rest.makeRequest("GET", `${requestURL}`)
    }
  }

  /**
   * @since 1.0.9
   * [Nodelink](https://nodelink.js.org) Mixer API, allows you to add/remove/update mix layers on a player if the node is hosted on a Nodelink Server.
   */
  mixer = {
    check: () => {
      return this.info?.isNodelink ?? false;
    },

    /**
     * @param {string} guildId
     * @param {import("..").AddMixLayerOptions} mixLayerOptions
     * @returns
     */
    addMixLayer: async (guildId, mixLayerOptions) => {
      if (!this.mixer.check()) {
        throw new Error("This node is not a Nodelink Server");
      }

      if (mixLayerOptions && typeof mixLayerOptions !== "object") {
        throw new TypeError("mixLayerOptions must be an object");
      }

      if (mixLayerOptions.track && typeof mixLayerOptions.track !== "object") {
        throw new TypeError("mixLayerOptions.track must be an object");
      }

      if (mixLayerOptions.track.encoded && mixLayerOptions.track.identifier) {
        throw new TypeError("mixLayerOptions.track.encoded and mixLayerOptions.track.identifier cannot be provided at the same time");
      }

      if (mixLayerOptions.volume !== undefined && typeof mixLayerOptions.volume !== "number" || mixLayerOptions.volume < 0 || mixLayerOptions.volume > 1) {
        throw new TypeError("mixLayerOptions.volume must be a number between 0 and 1");
      }

      const body = {
        track: mixLayerOptions.track
      }

      if (mixLayerOptions.volume) {
        body.volume = mixLayerOptions.volume;
      }


      return this.rest.makeRequest("POST", `/v4/sessions/${this.sessionId}/players/${guildId}/mix`, body)
    },

    getActiveMixLayers: async (guildId) => {
      if (!this.mixer.check()) {
        throw new Error("Node is not hosted with Nodelink Server");
      }
      return await this.rest.makeRequest("GET", `/v4/sessions/${this.sessionId}/players/${guildId}/mix`);
    },

    updateMixLayerVolume: async (guildId, mixId, volume) => {
      if (!this.mixer.check()) {
        throw new Error("Node is not hosted with Nodelink Server");
      }
      if (!guildId || !mixId || !volume) {
        throw new TypeError("guildId, mixId and volume are required to Update Mix Volume");
      }

      if (mixId !== undefined && typeof mixId !== "string") {
        throw new TypeError("id must be a string");
      }

      if (volume !== undefined && typeof volume !== "number" || volume < 0 || volume > 1) {
        throw new TypeError("volume must be a number between 0 and 1");
      }

      return await this.rest.makeRequest("PATCH", `/v4/sessions/${this.sessionId}/players/${guildId}/mix/${mixId}`, { volume });
    },

    removeMixLayer: async (guildId, mixId) => {
      if (!this.mixer.check()) {
        throw new Error("Node is not hosted with Nodelink Server");
      }
      if (!guildId || !mixId) {
        throw new TypeError("guildId and mixId are required to Remove the Mix Layer");
      }

      if (mixId !== undefined && typeof mixId !== "string") {
        throw new TypeError("id must be a string");
      }

      return await this.rest.makeRequest("DELETE", `/v4/sessions/${this.sessionId}/players/${guildId}/mix/${mixId}`);
    }
  }

  /**
   * [Nodelink-Only](https://nodelink.js.org)
   * SponsorBlock API
   */

  sponsorBlock = {
    /**
     * @internal
     *
     * Validates the provided segment object, throws an error if it's invalid (as everything is required)
     */
    _validateSponsorBlockSegment: (segment) => {
      if (typeof segment !== "object") {
        throw new TypeError("segment must be an object");
      }

      if(typeof segment.uuid !== "string") throw new TypeError("segment.uuid must be a string");
      if(typeof segment.start !== "number") throw new TypeError("segment.start must be a number");
      if(typeof segment.end !== "number") throw new TypeError("segment.end must be a number");
      // (e.g. sponsor, intro, outro, selfpromo, interaction, preview, music_offtopic, filler).
      if(typeof segment.category !== "string") throw new TypeError("segment.category must be a string");
      // (e.g. skip, mute, poi, chapter).
      if(typeof segment.actionType !== "string") throw new TypeError("segment.actionType must be a string");
      if(typeof segment.votes !== "number") throw new TypeError("segment.votes must be a number");
      if(typeof segment.locked !== "boolean") throw new TypeError("segment.locked must be a boolean");
      if(typeof segment.videoDuration !== "number") throw new TypeError("segment.videoDuration must be a number");
      if(typeof segment.description !== "string") throw new TypeError("segment.description must be a string");
    },

    check: () => {
      return this.info?.isNodelink ?? false;
    },

    /**
     * Returns the current SponsorBlock state for a player.
     * @param {string} guildId
     */
    getCurrentBlock: async (guildId) => {
      if(!this.sponserBlock.check()) throw new Error("This node is not a Nodelink Server");

      if (typeof guildId !== "string") throw new TypeError("guildId must be a string");

      return await this.rest.makeRequest("GET", `/v4/sessions/${this.sessionId}/players/${guildId}/sponsorblock`);
    },


    /**
     *
     * Updates SponsorBlock settings for a player. Only the provided options are changed.
     * @link https://nodelink.js.org/docs/api/rest#updatesponsorblock
     *
     * @param {string} guildId
     * @param {object} options
     * @returns {Promise<object>}
     * @throws {Error} If the node is not a Nodelink Server.
     * @throws {TypeError} If the provided options are of invalid types or values.
     */
    updateSettings: async (guildId, options) => {
      if (!this.sponserBlock.check()) {
        throw new Error("This node is not a Nodelink Server");
      }

      if(!options || typeof options !== "object") {
        throw new TypeError("Options must be an object");
      }

      if(typeof options.enabled !== "boolean") {
        throw new TypeError("options.enabled must be a boolean");
      }

      if (!Array.isArray(options.categories) || options.categories.some(category => typeof category !== "string")) {
        throw new TypeError("options.categories must be an array of strings");
      }

      if(options.actionTypes !== undefined && (!Array.isArray(options.actionTypes) || options.actionTypes.some(actionType => typeof actionType !== "string"))) {
        throw new TypeError("options.actionTypes must be an array of strings");
      }

      if (options.skipMarginMs !== undefined && (typeof options.skipMarginMs !== "number" || options.skipMarginMs < 0)) {
        throw new TypeError("options.skipMarginMs must be a positive number");
      }

      return await this.rest.makeRequest("PATCH", `/v4/sessions/${this.sessionId}/players/${guildId}/sponsorblock`, options);
    },

    /**
     * Overrides the segments array for a player with a custom set of segments.
     * @link https://nodelink.js.org/docs/api/rest#setsponsorblocksegments
     *
     * @param {string} guildId
     * @param {Array} segments
     */
    setBlockSegments: async (guildId, segments) => {

      if(!this.sponserBlock.check()) {
        throw new Error("This node is not a Nodelink Server");
      }

      if (typeof guildId !== "string") {
        throw new TypeError("guildId must be a string");
      }

      if(!Array.isArray(segments)) {
        throw new TypeError("segments must be an array");
      }

      segments.forEach(segment => this.sponserBlock._validateSponsorBlockSegment(segment));

      return await this.rest.makeRequest("PUT", `/v4/sessions/${this.sessionId}/players/${guildId}/sponsorblock`, { segments });
    },

    /**
     * Clears all SponsorBlock state for a player (segments, last skipped UUID, and resets to defaults).
     * @link https://nodelink.js.org/docs/api/rest#clearsponsorblock
     *
     * @param {string} guildId
     */
    clearSponsorBlock: async (guildId) => {
      if(!this.sponserBlock.check()) {
        throw new Error("This node is not a Nodelink Server");
      }

      if (typeof guildId !== "string") {
        throw new TypeError("guildId must be a string");
      }

      return await this.rest.makeRequest("DELETE", `/v4/sessions/${this.sessionId}/players/${guildId}/sponsorblock`);
    }
  }

  /**
   * [Nodelink-Only](https://nodelink.js.org) & works when `enableTrackStreamEndpoint` config option is enabled on the Nodelink Server.
   *
   * @description Retrives the Source's Audio Stream URL & formats for the provided encoded track string and itag.
   *
   * **Note:** This method is only for fetching the audio source URL for a track, it doesn't actually fetch or return the audio stream itself, you can use the returned URL to directly stream the audio from the source.
   * @param {string} encodedTrackStr The Encoded Track String of the track to fetch the stream for.
   * @param {number} itag The itag of the source to fetch
   * @returns {Promise<string>} The Audio Source URL for the provided track and (optionally) itag.
   * @throws {Error} If the node is not a Nodelink Server.
   * @see https://nodelink.js.org/docs/api/nodelink-features#direct-streaming
   */
  async fetchTrackStream(encodedTrackStr, itag = null) {

    if(!this.info?.isNodelink) {
      throw new Error("This node is not a Nodelink Server");
    }

    if(!encodedTrackStr || typeof encodedTrackStr !== "string") {
      throw new TypeError(`encodedTrackStr must be a string, received ${encodedTrackStr}`);
    }

    if (itag !== null && (typeof itag !== "number" || itag < 0)) {
      throw new TypeError(`itag must be a positive number, received ${itag}`);
    }

    return await this.rest.makeRequest("GET", `/v4/trackstream?encodedTrack=${encodedTrackStr}${itag ? `&itag=${itag}` : ""}`);
  }

  /**
   * [Nodelink-Only](https://nodelink.js.org) & works when `enableLoadStreamEndpoint` config option is enabled on the Nodelink Server.
   *
   * Stream raw PCM audio for custom processing or recording.
   *
   * @param {string} encodedTrackStr
   * @param {number|null} volume
   * @param {number|null} position
   * @param {string|object|null} filters
   * @returns {Promise<ReadableStream>} Readable Stream of raw PCM audio data for the provided track, volume, position, and filters.
   * @throws {Error} If the node is not a Nodelink Server.
   * @throws {TypeError} If the provided parameters are of invalid types or values.
   * @see https://nodelink.js.org/docs/api/nodelink-features#pcm-streaming
   */
  async fetchPCMStream(encodedTrackStr, volume = null, position = null, filters = null) {

    if(!this.info?.isNodelink) {
      throw new Error("This node is not a Nodelink Server");
    }

    if(!encodedTrackStr || typeof encodedTrackStr !== "string") {
      throw new TypeError(`encodedTrackStr must be a string, received ${encodedTrackStr}`);
    }

    if (volume !== null && (typeof volume !== "number" || volume < 0 || volume > 1000)) {
      throw new TypeError(`volume must be a null or number between 0 and 1000, received ${volume}`);
    }

    if (position !== null && (typeof position !== "number" || position < 0)) {
      throw new TypeError(`position must be a null or positive number, received ${position}`);
    }

    if (filters !== null && typeof filters !== "string" && typeof filters !== "object") {
      throw new TypeError(`filters must be a null, string or object, received ${filters}`);
    }

    const body = {
      encodedTrack: encodedTrackStr,
    }

    if (volume !== null) body.volume = volume;
    if (position !== null) body.position = position;
    if (filters !== null) body.filters = filters;

    return await this.rest.makeRequest("POST", `/v4/loadstream`, body);
  }

  /**
   * [Nodelink-Only](https://nodelink.js.org)
   *
   * Retrieves chapter markers from YouTube videos.
   *
   * @link https://nodelink.js.org/docs/api/nodelink-features#chapters-api
   *
   * @param {string} encodedTrackStr
   */
  async loadChapters(encodedTrackStr) {

    if(!this.info?.isNodelink) {
      throw new Error("This node is not a Nodelink Server");
    }

    if(!encodedTrackStr || typeof encodedTrackStr !== "string") {
      throw new TypeError(`encodedTrackStr must be a string, received ${encodedTrackStr}`);
    }

    return await this.rest.makeRequest("GET", `/v4/loadchapters?encodedTrack=${encodedTrackStr}`);

  }

  /**
   * @typedef {Object} fetchInfoOptions
   * @property {import("..").Version} [restVersion] The Rest Version to fetch info the from, Default: one set in the constructor(Node.restVersion)
   * @property {boolean} [includeHeaders=false] Whether to include headers in the response returned.
   *
   * @param {fetchInfoOptions} options
   */
  async fetchInfo(options = { restVersion: this.restVersion, includeHeaders: false }) {

    return await this.rest.makeRequest("GET", `/${options.restVersion || this.restVersion}/info`, null, options.includeHeaders)
  }

  // /**
  //  * Fetches Lavalink Node's Version and checks If it's supported by Riffy (v3 and v4)
  //  * Destroys the Lavalink Node if it's not supported.
  //  * @todo Probably to wait until version checks are completed before continuing to connnect to Lavalink.
  //  * @todo Add option to skip the version checks in-case needed.
  //  * @private
  //  */
  // async #fetchAndCheckVersion() {
  //     console.log(this.restVersion == "v3" ? "v4" : "v3")
  //     await Promise.all([this.fetchInfo({ includeHeaders: true }), this.fetchInfo({ restVersion: this.restVersion == "v3" ? "v4" : "v3", includeHeaders: true })]).then(([restVersionRequest, flippedRestRequest]) => {
  //         console.log(restVersionRequest, flippedRestRequest)
  //         /**
  //          * Lavalink Node's Version that was fetched, checks and uses the succeeded request
  //          * Uses `lavalink-api-version` header if `major` property isn't available/is `0` in the request, it can use either one variable. Defaults to `0` if `lavalink-api-version` isn't available.
  //          */
  //         console.log((
  //             ("version" in restVersionRequest?.data && restVersionRequest.data) ||
  //             flippedRestRequest?.data
  //         ).version)
  //         const nodeFetchedVersionObj = Object.assign(
  //             (
  //                 ("version" in restVersionRequest?.data && restVersionRequest.data) ||
  //                 flippedRestRequest?.data
  //             ).version,
  //             {
  //                 major: !(restVersionRequest?.data?.version || flippedRestRequest?.data?.version)?.major
  //                     ? Number(
  //                         (restVersionRequest || flippedRestRequest).headers.get("lavalink-api-version")
  //                     ) || 0
  //                     : (restVersionRequest?.data?.version || flippedRestRequest?.data?.version)?.major,
  //             }
  //         );

  //         if (restVersionRequest?.data?.status == 404) this.riffy.emit(
  //             "debug",
  //             `[Node (${this.name}) - Version Check] ${this.restVersion
  //             } set By User/Defaulted Version Check Failed, attempted ${this.restVersion == "v3" ? "v4" : "v3"
  //             } For version Checking`
  //         );

  //         if (flippedRestRequest?.data?.status === 404 && restVersionRequest?.data?.status === 404) {
  //             this.riffy.emit("debug", `[Node (${this.name}) - Version Check] Both Version Checks failed, Disconnecting Gracefully & Throwing Error`)

  //             // Disconnect Websocket & Destroy the players(if any created - Just incase)
  //             this.destroy()

  //             throw new Error(`${this.name}(${this.host}) is using unsupported Lavalink Version, Supported Lavalink Versions are v3 and v4.`)
  //         }

  //         if (restVersionRequest?.data?.status !== 404 || flippedRestRequest?.data?.status !== 404) {
  //             this.riffy.emit(
  //                 "debug",
  //                 `[Node (${this.name}) - Version Check] Check ${restVersionRequest?.status === 404 ? "Un" : ""}successful Lavalink Server uses ${nodeFetchedVersionObj.semver} ${restVersionRequest.status === 404 ? `Doesn't match with restVersion: ${this.restVersion}, Provided in Riffy Options` : ""}`
  //             );

  //             // If defaulted/user-specified fails Graceful Destroy/close the node's connection.
  //             if (restVersionRequest?.data?.status === 404) {
  //                 this.riffy.emit("debug", `[Node (${this.name}) - Version Check] Disconnecting Gracefully & Throwing Error`)

  //                 // Disconnect Websocket & Destroy the players(if any created - Just incase)
  //                 this.destroy()

  //                 throw new Error(`${this.name} is specified/defaulted to use ${this.restVersion}, but found using Lavalink version v${nodeFetchedVersionObj.major}, TIP: Set 'restVersion' property to "v${nodeFetchedVersionObj.major}" in Riffy Class's Options(Riffy Options)`);
  //             }
  //         }

  //         const { headers, ...restVersionRequestWithoutHeaders } = restVersionRequest;

  //         // If `restVersionRequest` isn't failed then update the `info` or set it back to empty Object.
  //         this.info = !("status" in restVersionRequest.data) ? restVersionRequestWithoutHeaders : {};
  //     }).catch((error) => {
  //         this.destroy()
  //         throw new Error("Failed to validate Lavalink Node's Version, possible causes: Lavalink Server is offline, Request Timeout.", { cause: error});
  //     })
  // }

  async connect() {
    if (this.ws) this.ws.close()
    // this.riffy.emit("debug", `[Node (${this.name}) - Version Check] Checking Node Version`);
    this.riffy.emit("debug", `[Node (${this.name})] Connecting to the Node (i.e Lavalink/Nodelink Server; Opening a WebSocket Connection)`);

    // // Preform Version Check To see If Lavalink Version is supported by Riffy (v3, v4)
    // await this.#fetchAndCheckVersion();

    const headers = {
      "Authorization": this.password,
      "User-Id": this.riffy.clientId,
      "Client-Name": `Riffy/${this.riffy.version}`,
    };

    if (this.restVersion === "v4") {
      if (this.sessionId) headers["Session-Id"] = this.sessionId;
    } else {
      if (this.resumeKey) headers["Resume-Key"] = this.resumeKey;
    }

    if (this.ws) {
      // Remove all listeners from the old socket so late events
      // (close/message/error) don't fire against the new connection.
      this.ws.removeAllListeners();
      this.ws = null;
      // Invalidate connection state — the old socket is gone. The new socket
      // will re-establish connected/_ready/sessionId through its own open()
      // and ready path. Without this, the node reports connected===true
      // with a stale sessionId until the new socket reaches ready, and
      // callers like lyrics.checkAvailable() issue REST requests against
      // the old (now invalid) session.
      this.connected = false;
      this._ready = false;
      this.sessionId = null;
      this.rest.sessionId = null;
    }
    this.ws = new Websocket(this.wsUrl, { headers });
    // been replaced by a newer reconnect. If WebSocket A is awaiting
    // fetchInfo() while reconnect creates WebSocket B, A's catch must NOT
    // close B — only close/replace if this.ws === socketA.
    const socket = this.ws;
    this.ws.on("open", () => {
      this.open(socket).catch((err) => {
        this.riffy.emit("debug", `[Node: ${this.name}] open() failed: ${err.message}`);
        this.riffy.emit("nodeError", this, err);
        // Only tear down if this is still the current socket — a newer
        // reconnect may have already replaced it.
        if (this.ws === socket) {
          this.connected = false;
    this._ready = false;
          this.ws?.close();
          this.ws = null;
        }
        if (!this._destroyed && this.ws !== socket) {
          // Socket was already replaced or cleaned up — don't reconnect
          // if a newer connection exists.
          if (!this.ws) this.reconnect();
        } else if (!this._destroyed) {
          this.reconnect();
        }
      });
    });
    // Capture socket for error handler — a late error from socket A must
    // NOT emit nodeError or trigger migration against the healthy socket B.
    this.ws.on("error", (event) => {
      this.error(event, socket);
    });
    // Capture socket for message handler — a late message from socket A
    // (e.g. a ready packet) must NOT overwrite sessionId/_ready or trigger
    // auto-resume when socket B has already replaced it.
    this.ws.on("message", (msg) => {
      this.message(msg, socket);
    });
    // Track the close() promise so destroy() can await it before removing players.
    // Capture the socket so close() can verify it hasn't been replaced by
    // a newer reconnect before mutating lifecycle state.
    this.ws.on("close", (event, reason) => {
      // GUARD 1 — stale socket: if this.ws has been replaced by a newer
      // reconnect, the closing socket is no longer current. Don't touch
      // _closePromise or start migration — the newer socket owns the
      // lifecycle now. Without this check, a queued close event from the
      // old socket would overwrite _closePromise (tracking an in-flight
      // disconnect() or previous close) with a promise that resolves
      // immediately (close() returns early for stale sockets), then clear
      // it in finally — leaving destroy() unable to await the real
      // in-flight migration, which can then PATCH the destination after
      // local players are removed (orphaned Lavalink players).
      if (this.ws !== socket) return;

      // GUARD 2 — already destroyed: no cleanup needed.
      if (this._destroyed) return;

      // GUARD 3 — serialize against an in-flight operation: if disconnect()
      // (or a previous close()) is already migrating players, its promise
      // is already registered in _closePromise. Don't overwrite it — that
      // operation owns the migration and will emit nodeDisconnect / close
      // the socket / call reconnect() when it completes. destroy() will
      // await THAT promise. Starting a second migration here would race
      // with the first and could PATCH the destination for already-moved
      // players.
      if (this._closePromise) return;

      // Register _closePromise BEFORE invoking close().
      //
      // close() emits nodeDisconnect synchronously (before its first
      // internal await on `this.riffy.migrate(this)`), and a listener
      // may call riffy.destroyNode() immediately during that emit.
      // destroy() checks this._closePromise to await the in-flight
      // migration before removing local players — otherwise the
      // in-flight moveTo() can PATCH destination players for the
      // already-removed locals, leaving orphaned Lavalink players.
      //
      // An async IIFE would run close() synchronously up to that
      // first await, which means nodeDisconnect fires BEFORE the
      // outer `this._closePromise = promise` assignment could run
      // — leaving _closePromise === null when the listener calls
      // destroyNode().
      //
      // Using a deferred promise lets us register it synchronously
      // first, then kick off close() which resolves/rejects it.
      //
      // Attach a no-op .catch() to the deferred promise itself: if close()
      // rejects (e.g., a synchronous nodeDisconnect/debug listener throws),
      // rejectClose is called → the deferred rejects. If nobody is awaiting
      // _closePromise yet (destroy() hasn't run), Node.js would report an
      // unhandled rejection — which on --unhandled-rejections=throw
      // terminates the process during socket cleanup. The .catch() swallows
      // the rejection until destroy() awaits it (destroy wraps the await in
      // try/catch). This does NOT lose the error: destroy() still sees the
      // settled state, and the close-chain's own rejection is handled by
      // the .catch() at the end of the .finally() chain below.
      let resolveClose, rejectClose;
      const promise = new Promise((res, rej) => { resolveClose = res; rejectClose = rej; });
      promise.catch(() => {});
      this._closePromise = promise;

      this.close(event, reason, socket)
        .then(resolveClose, rejectClose)
        .finally(() => {
          // Only clear if still ours — a newer close/disconnect may
          // have replaced it (shouldn't happen due to GUARD 3 above,
          // but defensive).
          if (this._closePromise === promise) {
            this._closePromise = null;
          }
        })
        // .finally() returns a NEW Promise. If close() rejected, this
        // Promise is also rejected and has no handler — swallow it.
        // The rejection was already delivered to the deferred promise
        // via rejectClose (and the deferred has its own .catch() above).
        .catch(() => {});
    });
  }

  async open(socket) {
    // If the socket has been replaced by a newer reconnect, abort.
    // Do NOT mutate connected/_ready — the newer socket owns the state now.
    // Only clear if this.ws is null (no replacement took over).
    if (this.ws !== socket) {
      if (this.ws === null) {
        this.connected = false;
        this._ready = false;
      }
      return;
    }

    if (this.reconnectAttempt) {
      clearTimeout(this.reconnectAttempt);
      this.reconnectAttempted = 1;
      this.reconnectAttempt = null;
    }

    // Set connected=true (WebSocket is open) but _ready=false — the node
    // is NOT selectable until the ready packet sets sessionId + _ready.
    this.connected = true;
    this._ready = false;
    this.riffy.emit('debug', `[Node: ${this.name}] Websocket connection established on ${this.wsUrl}`);

    const fetchedInfo =
          await this.fetchInfo()
            .then((info) => {
              if (this.ws !== socket) return null;
              return info;
            })
            .catch((e) => (this.riffy.emit('debug', `[Node: ${this.name}] Failed to fetch info on open: ${e.message}`)));

    // If the socket was replaced during fetchInfo(), abort. Do NOT clear
    // connected/_ready — the newer socket's open() already set its own state.
    // Clearing here would wipe the newer connection's valid state.
    if (this.ws !== socket) {
      return;
    }

    this.info = fetchedInfo;

    // @ts-ignore this.options exists on the constructor
    if (!this.info && !this.options?.bypassChecks?.nodeFetchInfo) {
      throw new Error(`Node (${this.name} - URL: ${this.restUrl}) Failed to fetch info on WS-OPEN`);
    }

    // connected stays true (WebSocket is open), but _ready is still false.
    // The node becomes selectable (_ready=true) only when the ready packet
    // arrives and sets sessionId.
  }

  error(event, socket) {
    if (!event) return;
    // If the socket that emitted this error has been replaced by a newer
    // reconnect, ignore it — don't emit nodeError or trigger migration
    // against the healthy replacement connection.
    if (socket && this.ws !== socket) return;
    this.riffy.emit("nodeError", this, event);
    this.riffy.emit("debug", `[Node: ${this.name}] Websocket Error: ${event.message || event}`);
    if (this.riffy.migrateOnFailure) {
      this.riffy.migrate(this).catch(err => {
        this.riffy.emit("debug", `Failed to auto-migrate players from node ${this.name} on error: ${err.message}`);
      });
    }
  }

  message(msg, socket) {
    // If the socket that emitted this message has been replaced by a newer
    // reconnect, ignore it — don't overwrite sessionId/_ready or trigger
    // auto-resume for a stale session.
    if (socket && this.ws !== socket) return;

    if (Array.isArray(msg)) msg = Buffer.concat(msg);
    else if (msg instanceof ArrayBuffer) msg = Buffer.from(msg);

    const payload = JSON.parse(msg.toString());
    if (!payload.op) return;

    this.riffy.emit("raw", "Node", payload);
    this.riffy.emit("debug", `[Node: ${this.name}] Received OP: ${payload.op} | Payload: ${JSON.stringify(payload)}`);

    if (payload.op === "stats") {
      this.stats = { ...payload };
      this.lastStats = Date.now();
    }

    if (payload.op === "ready") {
      // Re-check socket identity after the raw/debug event emissions above —
      // a listener could have replaced or destroyed the node during either.
      if (this._destroyed || (socket && this.ws !== socket)) return;

      if (this.sessionId !== payload.sessionId) {
        this.rest.setSessionId(payload.sessionId);
        this.sessionId = payload.sessionId;
      }
      this._ready = true;

      this.riffy.emit("nodeConnect", this);

      // Re-check after nodeConnect — a listener could destroy the node.
      if (this._destroyed || (socket && this.ws !== socket)) return;

      this.riffy.emit("debug", `[Node: ${this.name}] Ready (Ready Payload received)! Session ID: ${payload.sessionId}, ${this.info?.isNodelink ? `Nodelink ✨ (V${this.info?.version?.semver})` : ""}`);

      // Re-check after the debug emission above — a listener could destroy
      // the node or replace the socket during that event.
      if (this._destroyed || (socket && this.ws !== socket)) return;

      if (this.restVersion === "v4") {
        if (this.sessionId) {
          this.rest.makeRequest(`PATCH`, `/${this.rest.version}/sessions/${this.sessionId}`, { resuming: true, timeout: this.resumeTimeout }).catch((e) => {
            this.riffy.emit("debug", `[Node: ${this.name}] Session-resume PATCH failed (v4): ${e.message}`);
          });
          this.riffy.emit("debug", `[Node: ${this.name}] Resuming configured (v4).`);
        }
      } else {
        if (this.resumeKey) {
          this.rest.makeRequest(`PATCH`, `/${this.rest.version}/sessions/${this.sessionId}`, { resumingKey: this.resumeKey, timeout: this.resumeTimeout }).catch((e) => {
            this.riffy.emit("debug", `[Node: ${this.name}] Session-resume PATCH failed (v3): ${e.message}`);
          });
          this.riffy.emit("debug", `[Node: ${this.name}] Resuming configured (v3).`);
        }
      }

      if (this.autoResume && !payload.resumed) {
        for (const player of this.riffy.players.values()) {
          if (player.node === this) {
            player.restart().catch((err) => {
              this.riffy.emit("debug", `[Node: ${this.name}] autoResume restart failed for ${player.guildId}: ${err.message}`);
            });
          }
        }
      }
    }

    const player = this.riffy.players.get(payload.guildId);
    if (payload.guildId && player) player.emit(payload.op, payload);
  }

  async close(event, reason, socket, ...args) {
        reason = reason.toString();

    if (this._destroyed) return;

    // Check socket identity BEFORE emitting lifecycle events — if the
    // closing socket has been replaced by a newer reconnect, don't emit
    // nodeDisconnect (false disconnect for a healthy node).
    if (socket && this.ws !== socket) return;

    // Mark the node as unavailable for NEW connections BEFORE emitting
    // nodeDisconnect — a synchronous listener that calls
    // createConnection() would otherwise select this still-connected,
    // still-ready node (leastUsedNodes/bestNode/fetchRegion all filter
    // !node._migrating) and attach a player to the closing socket. That
    // player would not be included in the migration snapshot (taken
    // inside riffy.migrate(this) below) and would be orphaned after
    // cleanup. Keep connected=true so Player.moveTo() can still use
    // oldNode.rest.destroyPlayer() to delete old players.
    this._migrating = true;

    this.riffy.emit("nodeDisconnect", this, { code: event, reason: reason });
    this.riffy.emit("debug", `Connection with Lavalink closed with Error code : ${event || "Unknown code"}, reason: ${reason || "Unknown reason"}`);

    // Re-check lifecycle identity AFTER the synchronous nodeDisconnect
    // emit. A listener can call connect() (replacing this.ws with a new
    // socket) or destroyNode() (setting _destroyed=true) during that
    // event. If so, abort this close operation — the new socket owns
    // the lifecycle now. Without this check, the close continuation
    // would migrate players (interfering with the new connection's
    // state), set connected/_ready=false (tearing down the replacement),
    // and schedule a duplicate reconnect.
    if (this._destroyed || (socket && this.ws !== socket)) {
      // The new socket (or a destroyed node) owns the state now. Don't
      // touch _migrating/connected/_ready — the new socket's open()/
      // ready path will set them. Just clear _migrating if the socket
      // changed (the new socket isn't migrating yet).
      if (socket && this.ws !== socket) {
        this._migrating = false;
      }
      return;
    }

    try {
      if (this.riffy.migrateOnDisconnect) {
        try {
          await this.riffy.migrate(this);
        } catch (err) {
          this.riffy.emit("debug", `Failed to auto-migrate players from node ${this.name} on disconnect: ${err.message}`);
        }
      }
    } finally {
      // Re-check again after the async migration — a listener inside
      // migrate() (playerMigrated, nodeMigrated, etc.) or a concurrent
      // operation could have replaced the socket or destroyed the node.
      if (this._destroyed || (socket && this.ws !== socket)) {
        if (socket && this.ws !== socket) {
          this._migrating = false;
        }
        return;
      }
      // _closePromise is cleared by the wrapper in the close handler —
      // NOT here, to avoid racing with the wrapper's finally.
      this._migrating = false;
      this.connected = false;
      this._ready = false;
      if (!this._destroyed) {
        this.reconnect();
      }
    }
  }

  reconnect() {
    // Prevent multiple reconnect loops
    if (this.reconnectAttempt) return;

    this.reconnectAttempt = setTimeout(() => {
      if (this.reconnectAttempted >= this.reconnectTries) {
        const error = new Error(`Unable to connect with ${this.name} node after ${this.reconnectTries} attempts.`);

        this.riffy.emit("nodeError", this, error);
        // Check _destroyed after nodeError — a listener may have destroyed it.
        if (this._destroyed) return;
        // Clean destroy
        return this.destroy(true);
      }

      // Check _destroyed before reconnecting.
      if (this._destroyed) return;

      this.ws?.removeAllListeners();
      this.ws = null;
      this.riffy.emit("nodeReconnect", this);
      // Re-check _destroyed after nodeReconnect — a listener may have
      // destroyed the node during that synchronous event.
      if (this._destroyed) return;
      this.riffy.emit("debug", `[Node: ${this.name}] Reconnecting... Attempt ${this.reconnectAttempted}/${this.reconnectTries}`);
      this.reconnectAttempt = null;
      this.connect();
      this.reconnectAttempted++;
    }, this.reconnectTimeout);
  }

  /**
   * Destroys the node connection and cleans up resources.
   *
   * @param {boolean} [clean=false] - Determines if a clean destroy should be performed.
   *                                  ### If `clean` is `true`
   *                                  it removes all listeners and nullifies the websocket,
   *                                  emits a "nodeDestroy" event, and deletes the node from the nodes map.
   *                                  ### If `clean` is `false`
   *                                  it performs the full disconnect process which includes:
   *                                  - Destroying all players associated with this node.
   *                                  - Closing the websocket connection.
   *                                  - Removing all listeners and nullifying the websocket.
   *                                  - Clearing any reconnect attempts.
   *                                  - Emitting a "nodeDestroy" event.
   *                                  - Deleting the node from the node map.
   *                                  - Setting the connected state to false.
   */
  async destroy(clean = false) {
    this._destroyed = true;

    // If close()-triggered migration is in-flight, await it before
    // destroying players — otherwise the in-flight moveTo() could
    // PATCH the destination after we've removed the local player,
    // creating an orphaned Lavalink player with no local owner.
    if (this._closePromise) {
      try { await this._closePromise; } catch (_) { /* migration already failed */ }
    }
    if (clean) {
      // Terminal destroy after reconnect exhaustion. Player.destroy() now
      // catches REST DELETE failures, so we always attempt the DELETE
      // (best-effort) instead of skipRest=true which left orphaned Lavalink
      // players behind.
      this.riffy.players.forEach((player) => {
        if (player.node !== this) return;

        player.destroy(false); // try REST Delete — .catch() handles failure
        this.riffy.emit("playerDestroy", player);
      });
      if (this.ws) this.ws?.close(1000, "Clean Destroy");
      this.ws?.removeAllListeners();
      this.ws = null;
      clearTimeout(this.reconnectAttempt);
      this.reconnectAttempt = null;
      this.riffy.emit("nodeDestroy", this);
      this.riffy.nodeMap.delete(this.name);
      this.connected = false;
    this._ready = false;
      return;
    }

    // Always clean up associated players — even when already disconnected.
    // Player.destroy() now catches REST DELETE failures, so we always use
    // the normal path (no skipRest) and let .catch() handle unreachable nodes.
    this.riffy.players.forEach((player) => {
      if (player.node !== this) return;

      this.riffy.destroyPlayer(player.guildId);
    });

    this.ws?.close(1000, "destroy");
    this.ws?.removeAllListeners();
    this.ws = null;

    clearTimeout(this.reconnectAttempt);
    this.reconnectAttempt = null;

    this.riffy.emit("nodeDestroy", this);
    this.riffy.emit("debug", `[Node: ${this.name}] Destroyed.`);

    this.riffy.nodeMap.delete(this.name);
    this.connected = false;
    this._ready = false;
  }

  disconnect() {
    // If close()-triggered migration is already in-flight, don't start a
    // second migration — return the existing promise so callers can await
    // the same operation.
    if (this._closePromise) return this._closePromise;

    // Use a local variable so the finally clearing this._closePromise
    // doesn't race with the return statement. The promise is stored in
    // _closePromise synchronously (before any await), and cleared in
    // finally after _doDisconnect completes. Subsequent disconnect()
    // calls see null and can proceed.
    //
    // The .catch(() => {}) at the end swallows any rejection from the
    // finally chain — the rejection is already delivered to the caller
    // via the returned promise (callers do `await disconnect()` and
    // handle errors there). Without this catch, if _doDisconnect()
    // rejects, the Promise returned by the IIFE's .finally() would also
    // reject with no handler, producing an unhandled rejection that can
    // terminate the process on Node.js configs that treat unhandled
    // rejections as fatal.
    const promise = (async () => {
      try {
        await this._doDisconnect();
      } finally {
        // Only clear if this is still our promise — a newer operation
        // may have replaced it (shouldn't happen due to the guard above,
        // but defensive).
        if (this._closePromise === promise) {
          this._closePromise = null;
        }
      }
    })().catch(() => {});
    this._closePromise = promise;
    return promise;
  }

  async _doDisconnect() {
    if (!this.connected) return;
    if (this._destroyed) return;
    this._migrating = true;
    const movePromises = [];
    this.riffy.players.forEach((player) => {
      if (player.node == this) {
        const dest = [...this.riffy.nodeMap.values()]
          .filter(n => n.connected && n._ready && !n._migrating && n !== this)
          .sort((a, b) => a.penalties - b.penalties)[0];
        if (dest) {
          movePromises.push(
            player.moveTo(dest).catch((err) => {
              this.riffy.emit("debug", `[Node: ${this.name}] disconnect() moveTo failed for ${player.guildId}: ${err.message}`);
            })
          );
        }
      }
    });
    await Promise.allSettled(movePromises);
    this._migrating = false;
    this.ws?.close(1000, "destroy");
    this.ws?.removeAllListeners();
    this.ws = null;
    this.connected = false;
    this._ready = false;
    this.riffy.emit("nodeDisconnect", this);
  }

  get penalties() {
    let penalties = 0;
    if (!this.connected) return penalties;
    if (this.stats.players) {
      penalties += this.stats.players;
    }
    if (this.stats.cpu && this.stats.cpu.systemLoad) {
      penalties += Math.round(Math.pow(1.05, 100 * this.stats.cpu.systemLoad) * 10 - 10);
    }
    if (this.stats.frameStats) {
      if (this.stats.frameStats.deficit) {
        penalties += this.stats.frameStats.deficit;
      }
      if (this.stats.frameStats.nulled) {
        penalties += this.stats.frameStats.nulled * 2;
      }
    }
    return penalties;
  }
}

module.exports = { Node };
