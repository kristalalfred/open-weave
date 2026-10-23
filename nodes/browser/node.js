"use strict";

const PROTOCOL_VERSION = 11;
const HEARTBEAT_MS = 5000;
const POLL_MS = 2000;
const STALL_POLLS = 3;
const RETRY_MS = 5000;
const FAILED_AFTER_ATTEMPTS = 5;
const ICE_GATHER_MS = 2000;
const CAPTURE_MS = 4000;
const NODE_ID_KEY = "weave-node-id";
const RESOURCE_ID_MAX_LEN = 63;
const RESOURCE_ID_PATTERN = /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/;

const config = readConfig();
let fatal = null;
const tokenOwner = tokenNodeId(config.token);
const nodeId = readNodeId();
const hops = new Map();
let registered = false;
if (!validResourceId(nodeId)) {
  fatal = "node id must be 1–63 lowercase letters, digits, or hyphens and start and end with a letter or digit";
} else if (tokenOwner && tokenOwner !== nodeId) {
  fatal = `the token belongs to node ${tokenOwner}, not ${nodeId}`;
} else if (config.iceServers === null) {
  fatal = "ice must be a JSON array of RTCIceServer objects";
}

function readConfig() {
  const params = new URLSearchParams(location.hash.slice(1));
  return {
    southbound: (params.get("southbound") || "").replace(/\/+$/, ""),
    token: params.get("token") || "",
    media: params.get("media") === "video" ? { video: true } : { video: true, audio: true },
    node: params.get("node") || "",
    network: params.get("network") || "internet",
    iceServers: parseIceServers(params.get("ice")),
  };
}

function parseIceServers(text) {
  if (!text) return [];
  try {
    const servers = JSON.parse(text);
    if (Array.isArray(servers)) return servers;
  } catch {}
  return null;
}

function tokenNodeId(token) {
  const dot = token.indexOf(".");
  return dot > 0 ? token.slice(0, dot) : "";
}

function readNodeId() {
  if (config.node) return config.node;
  if (tokenOwner) return tokenOwner;
  let id = sessionStorage.getItem(NODE_ID_KEY);
  if (!id) {
    const bytes = crypto.getRandomValues(new Uint8Array(4));
    id = "browser-" + Array.from(bytes, b => b.toString(16).padStart(2, "0")).join("");
    sessionStorage.setItem(NODE_ID_KEY, id);
  }
  return id;
}

function validResourceId(value) {
  return value.length <= RESOURCE_ID_MAX_LEN && RESOURCE_ID_PATTERN.test(value);
}

async function southbound(method, path, body) {
  const headers = {};
  if (config.token) headers.authorization = `Bearer ${config.token}`;
  if (body !== undefined) headers["content-type"] = "application/json";
  return fetch(`${config.southbound}${path}`, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

function inputs() {
  return [{ id: "camera", label: "Camera", tracks: Object.keys(config.media).sort() }];
}

function outputs() {
  return [{ id: "player", label: "Player" }];
}

function registration() {
  return {
    protocol_version: PROTOCOL_VERSION,
    node: {
      id: nodeId,
      endpoint: `browser://${nodeId}`,
      status: "ready",
      capabilities: {
        adapters: [],
        hop_profiles: [
          {
            id: "camera-to-whip",
            ingress: INPUT_TRANSPORT,
            egress: { transport: "whip", roles: ["connect"] },
            max_egresses: 1,
            media_behavior: "unknown",
          },
          {
            id: "whep-to-display",
            ingress: { transport: "whep", roles: ["connect"] },
            egress: OUTPUT_TRANSPORT,
            max_egresses: 1,
            media_behavior: "unknown",
          },
        ],
      },
      topology: {
        attachments: [{ id: "client", network: config.network, dial: true, listeners: {} }],
      },
      inputs: inputs(),
      outputs: outputs(),
    },
    endpoints: [],
    hop_status: hopStatuses(),
  };
}

async function register() {
  const response = await southbound("POST", "/nodes/register", registration());
  if (response.status === 400 || response.status === 403 || response.status === 409) {
    fatal = `southbound refused registration: ${response.status} ${await response.text()}`;
    throw new Error(fatal);
  }
  if (!response.ok) throw new Error(`register: ${response.status} ${await response.text()}`);
  registered = true;
}

async function heartbeat() {
  if (fatal) return;
  try {
    if (!registered) {
      await register();
      return;
    }
    const response = await southbound("POST", `/nodes/${nodeId}/heartbeat`, {
      node_id: nodeId,
      status: "ready",
      inputs: inputs(),
      outputs: outputs(),
      endpoints: [],
      hop_status: hopStatuses(),
    });
    if (response.status === 404) registered = false;
    else if (!response.ok) throw new Error(`heartbeat: ${response.status}`);
  } catch (error) {
    registered = false;
    log(`southbound: ${error.message}`);
  } finally {
    render();
  }
}

async function pollDesired() {
  if (fatal || !registered) return;
  try {
    const response = await southbound("GET", `/nodes/${nodeId}/desired`);
    if (!response.ok) throw new Error(`desired: ${response.status}`);
    await reconcile(await response.json());
  } catch (error) {
    log(`desired: ${error.message}`);
  }
  for (const hop of hops.values()) await hop.observe();
  render();
}

async function reconcile(desired) {
  const wanted = new Map(desired.map(spec => [spec.id, spec]));
  for (const [id, hop] of hops) {
    const spec = wanted.get(id);
    if (!spec || JSON.stringify(spec) !== JSON.stringify(hop.spec)) {
      hop.stop();
      hops.delete(id);
    }
  }
  for (const [id, spec] of wanted) {
    if (hops.has(id)) continue;
    const hop = Hop.from(spec);
    hop.revision = await hopRevision(spec);
    hops.set(id, hop);
    hop.start();
  }
}

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value !== null && typeof value === "object") {
    return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

async function hopRevision(spec) {
  const bytes = new TextEncoder().encode("open-weave hop\0" + canonicalJson(spec));
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest), byte => byte.toString(16).padStart(2, "0")).join("");
}

function hopStatuses() {
  return Array.from(hops.values(), hop => hop.status());
}

class Progress {
  constructor() {
    this.last = null;
    this.lastAt = 0;
    this.stale = 0;
    this.ever = false;
    this.advanced = false;
    this.rateMbps = 0;
  }

  observe(bytes) {
    const now = performance.now();
    if (bytes == null) return;
    if (this.last != null) {
      if (bytes > this.last) {
        this.ever = true;
        this.stale = 0;
        this.advanced = true;
        const seconds = (now - this.lastAt) / 1000;
        this.rateMbps = seconds > 0 ? ((bytes - this.last) * 8) / seconds / 1e6 : 0;
      } else {
        this.stale += 1;
        this.advanced = false;
        this.rateMbps = 0;
      }
    }
    this.last = bytes;
    this.lastAt = now;
  }

  condition(connected) {
    if (!connected) return "connecting";
    if (this.ever && this.stale >= STALL_POLLS) return "stalled";
    if (this.advanced) return "flowing";
    return "connected";
  }
}

const INPUT_TRANSPORT = "input";
const OUTPUT_TRANSPORT = "output";

function socketName(socket) {
  if (!socket) return "nothing";
  return socket.id ? `${socket.transport} ${socket.id}` : socket.transport;
}

class Hop {
  static from(spec) {
    if (spec.egresses.length !== 1) {
      const branches = spec.egresses.map(egress => egress.branch_id).join(", ") || "none";
      return new UnsupportedHop(
        spec,
        `this node supports one egress per hop, not ${spec.egresses.length} (${branches})`,
      );
    }
    const egress = spec.egresses[0];
    if (
      spec.profile_id === "camera-to-whip" &&
      spec.ingress.transport === INPUT_TRANSPORT &&
      egress.transport === "whip"
    ) {
      return new SenderHop(spec);
    }
    if (
      spec.profile_id === "whep-to-display" &&
      spec.ingress.transport === "whep" &&
      egress.transport === OUTPUT_TRANSPORT
    ) {
      return new ReceiverHop(spec);
    }
    return new UnsupportedHop(spec);
  }

  constructor(spec) {
    this.spec = spec;
    this.revision = "";
    this.pc = null;
    this.resource = null;
    this.error = null;
    this.attempts = 0;
    this.retry = null;
    this.stopped = false;
    this.progress = new Progress();
    this.stats = null;
    this.codecs = [];
    this.connected = false;
  }

  async start() {
    try {
      await this.open();
      this.error = null;
      this.attempts = 0;
    } catch (error) {
      this.attempts += 1;
      this.error = error.message;
      log(`${this.spec.id}: ${error.message}`);
      this.close();
      if (!this.stopped) this.retry = setTimeout(() => this.start(), RETRY_MS);
    }
    render();
  }

  stop() {
    this.stopped = true;
    clearTimeout(this.retry);
    this.close();
  }

  close() {
    if (this.resource) {
      fetch(this.resource, { method: "DELETE" }).catch(() => {});
      this.resource = null;
    }
    if (this.pc) {
      this.pc.close();
      this.pc = null;
    }
    this.connected = false;
  }

  newPeerConnection() {
    const pc = new RTCPeerConnection({ iceServers: config.iceServers });
    pc.onconnectionstatechange = () => {
      if (["failed", "disconnected", "closed"].includes(pc.connectionState) && this.pc === pc) {
        log(`${this.spec.id}: connection ${pc.connectionState}; reconnecting`);
        this.close();
        if (!this.stopped) this.retry = setTimeout(() => this.start(), RETRY_MS);
      }
    };
    this.pc = pc;
    return pc;
  }

  async signal(url) {
    const pc = this.pc;
    await pc.setLocalDescription(await pc.createOffer());
    await iceGathered(pc, ICE_GATHER_MS);
    const response = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/sdp" },
      body: pc.localDescription.sdp,
    });
    if (!response.ok) throw new Error(`${url} → ${response.status} ${(await response.text()).trim()}`);
    const answer = await response.text();
    const location = response.headers.get("location");
    this.resource = location ? new URL(location, url).href : null;
    if (!this.resource) log(`${this.spec.id}: session created; Location not exposed to this origin, teardown relies on the server's timeout`);
    if (this.pc !== pc) return;
    await pc.setRemoteDescription({ type: "answer", sdp: answer });
  }

  state() {
    if (this.error) return this.attempts >= FAILED_AFTER_ATTEMPTS ? "failed" : "pending";
    return this.pc ? "provisioned" : "pending";
  }

  async observe() {
    if (!this.pc) {
      this.connected = false;
      this.stats = null;
      return;
    }
    this.connected = this.pc.connectionState === "connected";
    const report = await this.pc.getStats();
    const totals = { bytesSent: 0, bytesReceived: 0, packetsLost: 0, sentLost: 0, retransmittedSent: 0, retransmittedReceived: 0 };
    const codecs = new Map();
    for (const entry of report.values()) {
      if (entry.type === "codec") codecs.set(entry.id, entry.mimeType);
    }
    this.codecs = [];
    for (const entry of report.values()) {
      if (entry.type === "outbound-rtp") {
        totals.bytesSent += entry.bytesSent || 0;
        totals.retransmittedSent += entry.retransmittedPacketsSent || 0;
        this.codecs.push(`${codecs.get(entry.codecId) || entry.kind}↑${entry.bytesSent || 0}`);
      } else if (entry.type === "remote-inbound-rtp") {
        totals.sentLost += entry.packetsLost || 0;
      } else if (entry.type === "inbound-rtp") {
        totals.bytesReceived += entry.bytesReceived || 0;
        totals.packetsLost += entry.packetsLost || 0;
        totals.retransmittedReceived += entry.retransmittedPacketsReceived || 0;
        this.codecs.push(`${codecs.get(entry.codecId) || entry.kind}↓${entry.bytesReceived || 0}`);
      }
    }
    this.progress.observe(this.sending ? totals.bytesSent : totals.bytesReceived);
    this.stats = {
      connections: this.connected ? 1 : 0,
      rate_mbps: this.progress.rateMbps,
      packets_sent_lost: totals.sentLost,
      packets_retransmitted: totals.retransmittedSent,
      packets_received_lost: totals.packetsLost,
      packets_received_retransmitted: totals.retransmittedReceived,
    };
  }

  status() {
    const ingress = { condition: this.ingressCondition() };
    const egresses = this.spec.egresses.map((egress, index) => ({
      branch_id: egress.branch_id,
      condition: this.egressCondition(egress, index),
    }));
    if (this.stats) {
      if (this.sending && egresses[0]) egresses[0].stats = this.stats;
      else ingress.stats = this.stats;
    }
    const status = {
      id: this.spec.id,
      node_id: nodeId,
      revision: this.revision,
      state: this.state(),
      ingress,
      egresses,
    };
    return status;
  }
}

class SenderHop extends Hop {
  constructor(spec) {
    super(spec);
    this.sending = true;
    this.stream = null;
  }

  async open() {
    if (!this.stream) this.stream = await captureDevices();
    const pc = this.newPeerConnection();
    for (const track of this.stream.getTracks()) {
      pc.addTransceiver(track, { direction: "sendonly" });
    }
    await this.signal(this.spec.egresses[0].url);
  }

  stop() {
    super.stop();
    if (this.stream) {
      for (const track of this.stream.getTracks()) track.stop();
      this.stream = null;
    }
  }

  ingressCondition() {
    const tracks = this.stream ? this.stream.getTracks() : [];
    if (!tracks.some(t => t.readyState === "live")) return "idle";
    return tracks.some(t => !t.muted) ? "flowing" : "connected";
  }

  egressCondition() {
    return this.progress.condition(this.connected);
  }
}

class ReceiverHop extends Hop {
  constructor(spec) {
    super(spec);
    this.sending = false;
    this.video = document.createElement("video");
    this.video.autoplay = true;
    this.video.playsInline = true;
    this.video.muted = true;
    this.video.controls = true;
    this.figure = el("figure");
    this.figure.append(this.video, el("figcaption", null, spec.id));
  }

  async open() {
    const pc = this.newPeerConnection();
    pc.addTransceiver("video", { direction: "recvonly" });
    pc.addTransceiver("audio", { direction: "recvonly" });
    const stream = new MediaStream();
    pc.ontrack = event => {
      stream.addTrack(event.track);
      this.video.srcObject = stream;
      this.video.play().catch(() => {});
    };
    await this.signal(this.spec.ingress.url);
  }

  stop() {
    super.stop();
    this.video.srcObject = null;
    this.figure.remove();
  }

  ingressCondition() {
    return this.progress.condition(this.connected);
  }

  egressCondition() {
    if (!this.video.srcObject) return "connecting";
    return this.video.readyState >= 2 && !this.video.paused ? "flowing" : "connected";
  }
}

class UnsupportedHop extends Hop {
  constructor(spec, reason = null) {
    super(spec);
    this.reason = reason;
  }

  async start() {
    const egresses = this.spec.egresses.map(socketName).join(", ") || "nothing";
    const unsupported = `this node realises input→whip and whep→output only, not ${socketName(this.spec.ingress)}→${egresses}`;
    this.error = this.reason || unsupported;
    log(`${this.spec.id}: ${this.error}`);
    render();
  }

  state() {
    return "failed";
  }

  ingressCondition() {
    return "idle";
  }

  egressCondition() {
    return "idle";
  }
}

function captureDevices() {
  if (!navigator.mediaDevices) {
    return Promise.reject(
      new Error("no camera access: the page must be served from a secure context (https, or http on localhost)"),
    );
  }
  const constraints = config.media;
  return Promise.race([
    navigator.mediaDevices.getUserMedia(constraints),
    new Promise((_, reject) =>
      setTimeout(
        () => reject(new Error(`getUserMedia(${JSON.stringify(constraints)}) did not answer within ${CAPTURE_MS} ms`)),
        CAPTURE_MS,
      ),
    ),
  ]);
}

function iceGathered(pc, timeoutMs) {
  if (pc.iceGatheringState === "complete") return Promise.resolve();
  return new Promise(resolve => {
    const done = () => {
      pc.removeEventListener("icegatheringstatechange", check);
      resolve();
    };
    const check = () => {
      if (pc.iceGatheringState === "complete") done();
    };
    pc.addEventListener("icegatheringstatechange", check);
    setTimeout(done, timeoutMs);
  });
}

const logLines = [];
function log(line) {
  logLines.unshift(`${new Date().toLocaleTimeString()} ${line}`);
  logLines.length = Math.min(logLines.length, 20);
}

function manifests() {
  const cameraName = derivedStreamName("cam");
  const returnName = derivedStreamName("return");
  return `name: ${cameraName}
source:
  input:
    node: ${nodeId}
    id: camera
destinations:
  - id: studio
    srt:
      node: strom-node-1

name: ${returnName}
source:
  srt:
    node: strom-node-1
destinations:
  - id: player
    output:
      node: ${nodeId}
      id: player`;
}

function derivedStreamName(suffix) {
  const full = `${nodeId}-${suffix}`;
  if (full.length <= RESOURCE_ID_MAX_LEN) return full;

  let hash = 2166136261;
  for (const char of nodeId) hash = Math.imul(hash ^ char.charCodeAt(0), 16777619);
  const tail = `-${(hash >>> 0).toString(16).padStart(8, "0")}-${suffix}`;
  return `${nodeId.slice(0, RESOURCE_ID_MAX_LEN - tail.length)}${tail}`;
}

function el(tag, cls, text) {
  const node = document.createElement(tag);
  if (cls) node.className = cls;
  if (text != null) node.textContent = text;
  return node;
}

function render() {
  document.getElementById("node-id").textContent = nodeId;
  document.getElementById("southbound").textContent = config.southbound || "(no #southbound= in the URL)";
  const state = document.getElementById("registration");
  state.textContent = fatal ? "rejected" : registered ? "registered" : "registering";
  state.className = `pill ${fatal ? "bad" : registered ? "good" : "wait"}`;
  document.getElementById("fatal").textContent = fatal || "";
  document.getElementById("manifests").textContent = manifests();

  const rows = document.getElementById("hops");
  rows.replaceChildren();
  if (hops.size === 0) rows.append(el("div", "empty", "no hops desired for this node"));
  for (const hop of hops.values()) {
    const status = hop.status();
    const row = el("div", "hop");
    row.append(
      el("span", "id", hop.spec.id),
      el("span", `pill ${status.state}`, status.state),
      el("span", "cond", `${hop.spec.ingress.transport} ${status.ingress.condition}`),
    );
    for (const [index, egress] of hop.spec.egresses.entries()) {
      row.append(
        el("span", "arrow", "→"),
        el(
          "span",
          "cond",
          `${egress.transport} ${status.egresses[index].condition} (${egress.branch_id})`,
        ),
      );
    }
    if (hop.stream) {
      row.append(el("span", "cond", hop.stream.getTracks().map(t => t.kind).join("+")));
    }
    const stats = hop.sending ? status.egresses[0]?.stats : status.ingress.stats;
    if (stats) {
      row.append(el("span", "rate", `${stats.rate_mbps.toFixed(2)} Mb/s`));
      if (hop.codecs && hop.codecs.length) row.append(el("span", "rate", hop.codecs.join(" ")));
    }
    if (hop.error) row.append(el("span", "err", hop.error));
    rows.append(row);
  }

  const videos = document.getElementById("videos");
  for (const hop of hops.values()) {
    if (hop instanceof ReceiverHop && !hop.figure.isConnected) {
      videos.append(hop.figure);
    }
  }
  document.getElementById("log").textContent = logLines.join("\n");
}

document.getElementById("copy").addEventListener("click", () => {
  navigator.clipboard.writeText(manifests()).catch(() => {});
});

render();
if (fatal) {
  render();
} else if (!config.southbound) {
  fatal = "open this page as index.html#southbound=http://host:8081&token=…";
  render();
} else {
  heartbeat();
  setInterval(heartbeat, HEARTBEAT_MS);
  setInterval(pollDesired, POLL_MS);
}
