import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { dirname, extname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const args = parseArgs(process.argv.slice(2));
const checkOnly = Boolean(args["check-only"]);
await checkDeclaredConstants(checkOnly);
if (checkOnly) process.exit(0);

const southbound = args.southbound || process.env.WEAVE_SOUTHBOUND_URL;
const token = args.token ?? process.env.WEAVE_SOUTHBOUND_TOKEN ?? "";
if (!southbound) {
  console.error("usage: check.mjs --southbound URL [--token T] [--page URL] [--serve PORT] [--stay]");
  process.exit(2);
}

const TYPES = { ".html": "text/html", ".js": "text/javascript", ".mjs": "text/javascript" };
let server = null;
if (args.serve) {
  server = createServer(async (req, res) => {
    const file = servedFile(req.url);
    if (!file) {
      res.writeHead(403);
      res.end();
      return;
    }
    try {
      const body = await readFile(file);
      res.writeHead(200, { "content-type": TYPES[extname(file)] || "application/octet-stream" });
      res.end(body);
    } catch {
      res.writeHead(404);
      res.end();
    }
  });
  await new Promise((ready, reject) => {
    server.once("error", reject);
    server.listen(Number(args.serve), "127.0.0.1", ready);
  }).catch(error => {
    console.error(`cannot serve on port ${args.serve}: ${error.message}`);
    process.exit(2);
  });
}
const page = args.page || (server ? `http://127.0.0.1:${args.serve}/` : null);
if (!page) {
  console.error("give --page URL, or --serve PORT to host nodes/browser here");
  process.exit(2);
}

const { chromium } = await import("playwright");

const executablePath = args.executable || process.env.WEAVE_BROWSER_EXECUTABLE;
const browser = await chromium.launch({
  ...(executablePath ? { executablePath } : { channel: "chromium" }),
  headless: !args.headed,
  args: [
    "--use-fake-device-for-media-stream",
    "--use-fake-ui-for-media-stream",
    "--autoplay-policy=no-user-gesture-required",
  ],
});
const context = await browser.newContext({ permissions: ["camera", "microphone"] });
const tab = await context.newPage();
tab.on("console", message => console.log(`[page] ${message.text()}`));
tab.on("pageerror", error => console.log(`[page error] ${error.message}`));
const videoOnly = args["video-only"] || process.env.WEAVE_BROWSER_MEDIA === "video";
const media = videoOnly ? "&media=video" : "";
const iceServers = typeof args.ice === "string" ? `&ice=${encodeURIComponent(args.ice)}` : "";
const pinned = args.node ? `&node=${encodeURIComponent(args.node)}` : "";
const registeredHere = tab
  .waitForResponse(
    response =>
      response.request().method() === "POST" &&
      new URL(response.url()).pathname.endsWith("/nodes/register") &&
      response.ok(),
    { timeout: 30_000 },
  )
  .then(() => true, () => false);
await tab.goto(`${page}#southbound=${encodeURIComponent(southbound)}&token=${encodeURIComponent(token)}${media}${pinned}${iceServers}`);
const nodeId = await tab.locator("#node-id").textContent();
console.log(`page open as ${nodeId}`);

if (!(await registeredHere)) {
  console.error(`${nodeId}: southbound accepted no registration from this page within 30s`);
  await browser.close();
  process.exit(1);
}

const deadline = Date.now() + 30_000;
let seen = false;
while (Date.now() < deadline) {
  const response = await fetch(`${southbound}/nodes`, {
    headers: token ? { authorization: `Bearer ${token}` } : {},
  }).catch(() => null);
  if (response && response.ok) {
    const nodes = await response.json();
    if (nodes.some(resource => resource.node.id === nodeId)) {
      seen = true;
      break;
    }
  }
  await new Promise(resolve => setTimeout(resolve, 1000));
}
if (!seen) {
  console.error(`${nodeId} did not appear in ${southbound}/nodes within 30s`);
  await browser.close();
  process.exit(1);
}
console.log(`${nodeId} registered with ${southbound}`);

if (args.stay) {
  console.log("staying up; hop status:");
  setInterval(async () => {
    const hops = await tab.locator("#hops").innerText().catch(() => "");
    const last = await tab.locator("#log").innerText().catch(() => "");
    console.log(hops.replace(/\n+/g, " | ") || "(no hops)", "::", last.split("\n")[0] || "");
  }, 5000);
  await new Promise(() => {});
} else {
  await browser.close();
  server?.close();
}

function servedFile(url) {
  let path;
  try {
    path = decodeURIComponent(url.split("?")[0].split("#")[0]);
  } catch {
    return null;
  }
  const file = resolve(here, "." + (path === "/" ? "/index.html" : path));
  return file === here || file.startsWith(here + sep) ? file : null;
}

async function checkDeclaredConstants(required) {
  const DRIFT_CHECKS = [
    {
      what: "protocol version",
      page: [/^const PROTOCOL_VERSION = (\d+);$/m, "const PROTOCOL_VERSION = <n>;"],
      core: [/^pub const PROTOCOL_VERSION: u32 = (\d+);$/m, "pub const PROTOCOL_VERSION: u32 = <n>;"],
    },
    {
      what: "input transport",
      page: [/^const INPUT_TRANSPORT = "([^"]+)";$/m, 'const INPUT_TRANSPORT = "<tag>";'],
      core: [/^pub const INPUT_TRANSPORT: &str = "([^"]+)";$/m, 'pub const INPUT_TRANSPORT: &str = "<tag>";'],
    },
    {
      what: "output transport",
      page: [/^const OUTPUT_TRANSPORT = "([^"]+)";$/m, 'const OUTPUT_TRANSPORT = "<tag>";'],
      core: [/^pub const OUTPUT_TRANSPORT: &str = "([^"]+)";$/m, 'pub const OUTPUT_TRANSPORT: &str = "<tag>";'],
    },
  ];

  const pagePath = join(here, "node.js");
  const corePath = resolve(here, "..", "..", "crates", "core", "src", "lib.rs");

  const pageSource = await read(pagePath);
  if (pageSource === null) fail(`cannot read ${pagePath}`);
  checkHopStatusContract(pageSource, pagePath);
  checkIceServers(pageSource, pagePath);
  await checkHopRevision(pageSource, pagePath);
  const coreSource = await read(corePath);

  for (const check of DRIFT_CHECKS) {
    const page = declaredValue(pageSource, check.page[0], pagePath, check.page[1]);
    if (coreSource === null) {
      const message = `node.js declares ${check.what} \`${page}\`, unchecked: cannot read ${corePath}`;
      if (required) fail(message);
      console.warn(message);
      continue;
    }
    const core = declaredValue(coreSource, check.core[0], corePath, check.core[1]);
    if (core !== page) {
      fail(`${check.what} mismatch: ${pagePath} declares \`${page}\`, ${corePath} declares \`${core}\``);
    }
    console.log(`${check.what} \`${page}\` matches ${corePath}`);
  }
}

function checkHopStatusContract(source, path) {
  if (!/spec\.egresses\.length !== 1/.test(source)) {
    fail(`${path} does not reject unsupported multi-egress hops`);
  }
  const status = /\n  status\(\) \{([\s\S]*?)\n  \}\n\}/.exec(source)?.[1];
  if (!status) fail(`${path} does not define Hop.status()`);

  for (const [pattern, field] of [
    [/const ingress = \{ condition:/, "an ingress status object"],
    [/this\.spec\.egresses\.map/, "one status per desired egress"],
    [/branch_id: egress\.branch_id/, "each desired branch id"],
    [/\begresses,/, "the egresses status array"],
    [/revision: this\.revision/, "the applied hop revision"],
  ]) {
    if (!pattern.test(status)) fail(`${path} Hop.status() does not report ${field}`);
  }
  if (/\begress:/.test(status)) fail(`${path} Hop.status() still reports one lossy egress`);
  if (!/rate_mbps: this\.progress\.rateMbps/.test(source)) {
    fail(`${path} does not report socket-level rate_mbps`);
  }
  if (/\b(?:ingress|egress)_rate_mbps:/.test(source)) {
    fail(`${path} still reports hop-level directional rates`);
  }
  console.log("browser hop status reports ingress and every identified egress branch");
}

async function checkHopRevision(source, path) {
  const canonical = /^function canonicalJson\(value\) \{\n([\s\S]*?)\n\}$/m.exec(source)?.[1];
  const revision = /^async function hopRevision\(spec\) \{\n([\s\S]*?)\n\}$/m.exec(source)?.[1];
  if (!canonical || !revision) fail(`${path} has no canonical hop revision`);
  const canonicalJson = new Function(`return function canonicalJson(value) {${canonical}}`)();
  const hash = new Function("canonicalJson", `return async function(spec) {${revision}}`)(canonicalJson);
  const first = { z: [1, { b: 2, a: "x" }], a: null };
  const second = { a: null, z: [1, { a: "x", b: 2 }] };
  const { createHash } = await import("node:crypto");
  const expected = createHash("sha256").update('open-weave hop\0{"a":null,"z":[1,{"a":"x","b":2}]}').digest("hex");
  if (await hash(first) !== expected || await hash(second) !== expected) fail(`${path} has an incompatible hop revision`);
  const hop = {
    id: "hop-a", node_id: "node-a", profile_id: "local", role: "sender",
    ingress: { transport: "input", id: "camera" },
    egresses: [{ branch_id: "studio", transport: "output", id: "player" }],
  };
  if (await hash(hop) !== "3863eff83ea855efb81128098118c8f26b03294081b15e0b02441ec7a66e516d") fail(`${path} does not match the Rust hop revision vector`);
  console.log("browser hop revisions hash canonical JSON");
}

function checkIceServers(source, path) {
  const body = /^function parseIceServers\(text\) \{\n([\s\S]*?)\n\}$/m.exec(source)?.[1];
  if (!body) fail(`${path} does not define parseIceServers()`);
  const parse = new Function("text", body);
  const server = [{ urls: ["turn:turn.example:3478"], username: "u", credential: "c" }];
  for (const [text, want] of [
    [null, []],
    ["", []],
    [JSON.stringify(server), server],
    ["not json", null],
    ['{"urls":"stun:x"}', null],
  ]) {
    if (JSON.stringify(parse(text)) !== JSON.stringify(want)) {
      fail(`${path} parseIceServers(${JSON.stringify(text)}) is not ${JSON.stringify(want)}`);
    }
  }
  if (!/new RTCPeerConnection\(\{ iceServers: config\.iceServers \}\)/.test(source)) {
    fail(`${path} does not pass config.iceServers to RTCPeerConnection`);
  }
  console.log("browser ice servers come from the page fragment and reach RTCPeerConnection");
}

function read(path) {
  return readFile(path, "utf8").catch(() => null);
}

function declaredValue(source, pattern, path, form) {
  const match = pattern.exec(source);
  if (!match) fail(`${path} does not declare \`${form}\``);
  return match[1];
}

function fail(message) {
  console.error(message);
  process.exit(1);
}

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (!arg.startsWith("--")) continue;
    const key = arg.slice(2);
    const next = argv[i + 1];
    if (next === undefined || next.startsWith("--")) out[key] = true;
    else out[key] = argv[++i];
  }
  return out;
}
