import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { test } from "node:test";
import { createContext, runInContext } from "node:vm";

class Element {
  constructor(tag) {
    this.tag = tag;
    this.className = "";
    this.childNodes = [];
    this.style = {};
  }

  append(...children) {
    this.childNodes.push(...children);
  }

  get textContent() {
    return this.childNodes.map(child => typeof child === "string" ? child : child.textContent).join("");
  }
}

const html = await readFile(new URL("./ui.html", import.meta.url), "utf8");
const script = html.match(/<script>([\s\S]*?)<\/script>/)[1].split("\nrenderLegend();")[0];

function render(stream) {
  const context = createContext({
    stream,
    document: {
      createElement: tag => new Element(tag),
      createTextNode: text => text,
    },
  });
  runInContext(script, context);
  return runInContext("renderPath(stream)", context);
}

function elements(root, className) {
  if (!(root instanceof Element)) return [];
  return [
    ...(root.className.split(" ").includes(className) ? [root] : []),
    ...root.childNodes.flatMap(child => elements(child, className)),
  ];
}

function socket(condition = "flowing", rate = 2.3) {
  return { transport: "srt", condition, stats: { rate_mbps: rate } };
}

function hop(id, node, role, branch, extra = {}) {
  return {
    id, node, role, state: "provisioned", ingress: socket(),
    egresses: [{ branch_id: branch, ...socket() }], ...extra,
  };
}

function stream() {
  return {
    name: "feed",
    endpoints: {
      ingress: { url: "srt://source:7000" },
      destinations: [{ id: "studio", endpoint: { node: "studio", url: "srt://studio:8000" } }],
    },
    hops: [
      hop("sender", "source", "sender", "studio", {
        egresses: ["studio", "studio.2"].map(branch_id => ({ branch_id, ...socket() })),
      }),
      hop("bridge-a", "relay-a", "bridge", "studio"),
      hop("receiver", "studio", "receiver", "studio", {
        ingress: socket("stalled", 0), merge_ingress: socket(),
        egresses: [{ branch_id: "studio", ...socket("flowing", 2.1) }],
      }),
      hop("bridge-b", "relay-b", "bridge", "studio.2"),
    ],
  };
}

test("both redundant paths join one receiver and one output when either path stalls", () => {
  for (const failed of [0, 1]) {
    const feed = stream();
    const receiver = feed.hops[2];
    receiver.ingress = socket(failed === 0 ? "stalled" : "flowing", failed === 0 ? 0 : 2.3);
    receiver.merge_ingress = socket(failed === 1 ? "stalled" : "flowing", failed === 1 ? 0 : 2.3);
    const graph = render(feed);
    assert.equal(elements(graph, "destination").length, 1);
    assert.equal(elements(graph, "redundant").length, 1);
    const branches = elements(graph, "branch");
    assert.equal(branches.length, 2);
    for (const [index, branch] of branches.entries()) {
      assert.match(branch.textContent, index === 0 ? /relay-a/ : /relay-b/);
      assert.equal(branch.childNodes.at(-1).style.color, index === failed ? "var(--red)" : "var(--green)");
    }
    const merger = elements(graph, "hop").filter(chip => chip.title.startsWith("receiver"));
    assert.equal(merger.length, 1);
    assert.match(merger[0].textContent, /merge@ studio/);
    assert.match(merger[0].textContent, /↓2.3 ↑2.1 Mb\/s/);
    const outputs = elements(graph, "addr").filter(chip => chip.textContent.startsWith("srt out"));
    assert.equal(outputs.length, 1);
    assert.match(outputs[0].textContent, /srt:\/\/studio:8000/);
  }
});

test("both receiver inputs contribute to the displayed ingress rate", () => {
  const feed = stream();
  feed.hops[2].ingress = socket("flowing", 2.2);
  const merger = elements(render(feed), "hop").find(chip => chip.title.startsWith("receiver"));
  assert.match(merger.textContent, /↓4.5 ↑2.1 Mb\/s/);
});

test("unreported merge input stays visible with an unknown condition", () => {
  const feed = stream();
  feed.hops[2].merge_ingress = { transport: "srt" };
  const input = elements(render(feed), "branch")[1].childNodes.at(-1);
  assert.equal(input.style.color, "var(--gray)");
  assert.match(input.className, /hollow/);
  assert.match(input.title, /input 2: unreported/);
});

test("fanout destinations retain separate receivers and outputs", () => {
  const feed = stream();
  feed.hops[0].egresses.push({ branch_id: "preview", ...socket() });
  feed.hops.push(hop("preview-receiver", "preview", "receiver", "preview"));
  feed.endpoints.destinations.push({ id: "preview", endpoint: { node: "preview", url: "srt://preview:9000" } });
  const graph = render(feed);
  assert.equal(elements(graph, "destination").length, 2);
  assert.equal(elements(graph, "branch").length, 3);
  assert.equal(elements(graph, "addr").length, 3);
});

test("single paths, remote listeners, and device outputs retain their endpoints", () => {
  for (const kind of ["single", "remote", "device"]) {
    const feed = stream();
    feed.hops[0].egresses.pop();
    feed.hops.pop();
    delete feed.hops[2].merge_ingress;
    if (kind === "remote") {
      feed.hops.pop();
      feed.endpoints.destinations[0].endpoint.node = "";
    }
    if (kind === "device") feed.hops[2].egresses[0] = { branch_id: "studio", transport: "device", mode: "playback" };
    const graph = render(feed);
    assert.equal(elements(graph, "branch").length, 1);
    assert.equal(elements(graph, "redundant").length, 0);
    assert.equal(elements(graph, "addr").length, 2);
    assert.match(graph.textContent, kind === "device" ? /screen/ : /srt:\/\/studio:8000/);
  }
});
