mod bench

default:
    @just --list

build:
    cargo build

test:
    cargo test --workspace
    just browser-check

fmt:
    cargo fmt

fmt-check:
    cargo fmt --all --check

contracts:
    cargo run -p weave-core --bin generate-contracts

contracts-check:
    cargo test -p weave-core contracts::tests

lint:
    cargo clippy --workspace --all-targets --all-features -- -D warnings

browser-check:
    node nodes/browser/check.mjs --check-only
    node --test crates/controller/src/ui.test.mjs

run-north:
    cargo run -p weave-northbound

run-south:
    cargo run -p weave-southbound

run-controller:
    cargo run -p weave-controller

run-strom-adapter *ARGS:
    cargo run -p weave-adapter-strom -- {{ARGS}}

run-node *ARGS:
    cargo run -p weave-media --bin weave-node -- {{ARGS}}

timed-demo ACTION="run" *ARGS: build
    cargo run -p weave-bench --bin weave-timed-demo -- {{ACTION}} {{ARGS}}

cli *ARGS:
    cargo run -p weave-cli -- {{ARGS}}

apply FILE="examples/stream.yaml":
    cargo run -p weave-cli -- apply -f {{FILE}}

apply-set OWNER FILE="examples/stream-set.yaml":
    cargo run -p weave-cli -- apply-set {{OWNER}} -f {{FILE}}

plan FILE="examples/stream.yaml":
    cargo run -p weave-cli -- plan -f {{FILE}}

get-streams:
    cargo run -p weave-cli -- get streams

get-stream NAME:
    cargo run -p weave-cli -- get stream {{NAME}}

get-nodes:
    cargo run -p weave-cli -- get nodes

get-status:
    cargo run -p weave-cli -- get status

get-endpoints NAME:
    cargo run -p weave-cli -- get endpoints {{NAME}}

delete-stream NAME:
    cargo run -p weave-cli -- delete stream {{NAME}}
