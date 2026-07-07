# Probing-RL — Online Observability for Agentic RL Training

<div align="center">
  <img src="probing.svg" alt="Probing Logo" width="200"/>

  <p>
    <a href="README.cn.md">中文</a> |
    <a href="README.md">English</a> |
    <a href="docs/probing-RL-agent-runtime.md">Agent Runtime Overview (汇报材料)</a>
  </p>
</div>

[![PyPI version](https://badge.fury.io/py/probing.svg)](https://badge.fury.io/py/probing)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Downloads](https://pepy.tech/badge/probing)](https://pepy.tech/project/probing)
[![codecov](https://codecov.io/gh/DeepLink-org/probing/graph/badge.svg?token=IRH3F0OI56)](https://codecov.io/gh/DeepLink-org/probing)

> See what slows down your RL training — not just GPU kernels.

**Probing-RL** extends [Probing](https://github.com/DeepLink-org/probing) into a **framework-neutral observability layer for Agentic RL**. It can attach to running agent runtimes and RL training stacks — including [Slime](https://github.com/THUDM/slime), [veRL](https://github.com/volcengine/verl), XTuner, and custom Ray-based systems — without replacing your training framework.

Where traditional profilers answer *what the GPU was doing*, Probing-RL answers *which rollout step, training phase, sample, turn, tool call, or reward step caused the slowdown* — in real time, across distributed processes.

---

## Why Agentic RL Needs a Different Profiler

Model-centric RL bottlenecks mostly live inside training and inference: kernel efficiency, communication, memory, and weight sync. **Agentic RL** adds a heterogeneous pipeline on top:

| Layer | Typical components | Common bottlenecks |
|-------|-------------------|-------------------|
| **Environment** | Sandbox, tool calls, external APIs | Concurrency limits, I/O blocking, tail episodes, queue buildup |
| **Inference** | SGLang, vLLM, custom engines | Rollout burstiness, growing context length, KV cache misses, straggler trajectories |
| **Training** | Actor/critic updates, buffer consumption | Rollout buffer starvation, phase mismatch with generation |
| **Reward** | Verifiers, group reward, online eval | Slow reward service blocking trajectory recycling |

These failures **propagate across systems**. A sandbox timeout can stall rollout workers, shrink the buffer, and leave GPUs idle — while kernel-level profilers still look healthy.

Existing framework profilers (Slime Rollout Timeline, veRL heavy/light profiling) are valuable but mostly **offline and framework-internal**. They struggle to:

- Observe **environment and tool** latency outside the training loop
- Correlate **sample / turn / phase** semantics with distributed Ray actors
- Provide **live** feedback during a long training run
- Work uniformly across Slime, veRL, XTuner, and bespoke agent runtimes

Probing-RL targets that gap: **online, cross-layer, sample-level observability** with a small integration contract.

---

## What Probing-RL Delivers

### For RL / Agent Framework Developers

- **Rollout timeline** — one row per sample/trajectory with phase bars (inference, env step, tool call, reward)
- **Train timeline** — per-batch training phases keyed by `train_step_id` or `batch_id`
- **Cross-process linking** — connect driver, rollout workers, and trainers via `rollout_id` + context carriers
- **Framework-neutral API** — `probing.rl` stores standard attributes on normal spans; no hard dependency on a specific RL stack
- **Agent runtime tool** — inject into a live process or start with `PROBING=1`; usable as an observability sidecar during RL jobs

### For Algorithm & Systems Engineers

- **Find straggler samples** — sort by end-to-end duration; pin a sample to inspect phase breakdown
- **Debug tail latency** — see whether slowness is inference, sandbox, tool, or reward dominated
- **Trace async RL** — `contextvars`-aware spans follow asyncio tasks and Ray remote calls
- **Drop to lower levels when needed** — span tree, process Gantt, Perfetto export, SQL queries on the same data

### Compared with Built-in Framework Profilers

| Capability | Slime / veRL built-in | Probing-RL |
|------------|----------------------|------------|
| GPU kernel / operator detail | Strong (Chrome Trace, Nsight, PyTorch Profiler) | Via Perfetto export + optional torch tracing |
| Rollout **logic** phase timing | Partial (framework-specific timeline) | First-class sample/turn/phase model |
| Environment / tool visibility | Limited | `env.step`, `tool.call` phases across processes |
| Online during training | Mostly offline trace dumps | Live Web UI + CLI |
| Framework binding | Per-framework | Slime, veRL, XTuner, custom — same contract |

Probing-RL complements — rather than replaces — framework profilers. Use both: framework tools for kernel-level tuning, Probing-RL for **end-to-end agent pipeline** diagnosis.

---

## Quick Start

### Installation

```bash
pip install probing
```

### Run the Slime-like Async RL Demo (≈2 minutes)

The repo includes a Ray async RL demo that mirrors `slime/train_async.py` without depending on Slime:

```bash
bash debug/test/ray_async_rl_demo.sh
```

Tune workload size:

```bash
NUM_ROLLOUTS=4 \
TRAJECTORIES_PER_ROLLOUT=64 \
TURNS_PER_TRAJECTORY=3 \
bash debug/test/ray_async_rl_demo.sh
```

Open the Web UI (default port from demo: `9922`). Go to **Rollout**, enter a `rollout_id`, and click **Load rollout**.

### Enable in Your RL Job

```bash
# At process startup
PROBING=1 python train.py

# Or attach to a running worker (Linux)
probing -t <pid> inject
```

Set `PROBING_ASSETS_ROOT` to the built Web UI assets when running inside Ray jobs (see demo script).

---

## Core RL Features

- **Rollout view** (`/rollout`) — per-trajectory phase timeline for one `rollout_id`
- **Train view** (`/train`) — per-batch training phases
- **Phase breakdown** — rollout rows show inference, env step, tool call, reward; train view shows train, optimizer, and custom phases
- **Pinned sample details** — click a sample to keep phase timing visible while scrolling
- **Ray-aware tracing** — process, actor, worker, and Ray identity on spans
- **Async-friendly context** — nested asyncio tasks keep rollout/sample context
- **SQL + Chrome trace export** — same span table powers RL views and lower-level analysis

### RL Trace Model

Standard attributes connect distributed work back to RL concepts:

```text
run_id, framework, algorithm, rollout_id, step_id, train_step_id,
sample_id, trajectory_id, group_id, group_index, attempt,
turn_id, env_step_id, phase, actor_role, process_role, batch_id
```

Recommended phases: `trajectory`, `inference`, `env.step`, `tool.call`, `reward`, `train.prepare`, `train.loss`, `optimizer.step`, `weights.update`, `checkpoint.save`, `eval`.

Full attribute rules and per-view requirements: see [Integration Contract](#integration-contract) below, or [readme.md](readme.md) for the detailed reference.

---

## Python API

```python
import probing.rl as rl

carrier = rl.bind(
    framework="slime",          # or "verl", "xtuner", your stack
    algorithm="grpo",
    rollout_id=12,
    sample_id="sample-42",
    actor_role="rollout",
)

with rl.context(**carrier):
    with rl.span("trajectory.sample", phase="trajectory"):
        with rl.span("inference.generate", phase="inference", turn_id=0):
            generate_tokens()

        with rl.span("env.step", phase="env.step", turn_id=0, env_step_id=0):
            step_environment()

        with rl.span("reward.compute", phase="reward"):
            compute_reward()
```

Async rollouts and Ray remote workers:

```python
async with rl.async_span("tool.call", phase="tool.call", turn_id=1):
    result = await call_tool()

carrier = rl.export_context()
remote_actor.train.remote(batch, carrier)

# on remote actor
carrier = rl.import_context(carrier, actor_role="trainer", batch_id="batch-12")
with rl.context(**carrier):
    with rl.span("batch.prepare", phase="train.prepare"):
        prepare_batch()
```

### Integration Checklist for a New Framework

1. Set `framework` and `rollout_id` at each rollout step.
2. Tag every trajectory with `trajectory_id` or `sample_id`.
3. Set `phase` on every timed boundary you care about.
4. On the trainer, add `train_step_id` or `batch_id` and `phase=train.*`.
5. Export/import the RL context carrier across async tasks and remote actors.
6. Open Web UI → **Rollout**, load a `rollout_id`.

Instrument these boundaries first: rollout submit/wait, per-sample lifecycle, agent turns, inference, env/tool, reward, batch prep, optimizer, weight update.

---

## Integration Contract

Any RL framework can drive the Probing UI via generic span attributes — the frontend never depends on framework-specific span names.

| Attribute | Required for | Description |
|-----------|--------------|-------------|
| `rollout_id` | Rollout, Train | One rollout generation cycle (`step_id` accepted as fallback) |
| `trajectory_id` or `sample_id` | Rollout | One row per trajectory in Rollout view |
| `train_step_id` or `batch_id` | Train | One row per batch in Train view |
| `phase` | Rollout, Train | e.g. `inference`, `env.step`, `tool.call`, `reward`, `train.loss` |
| `actor_role` | Cross-process | `driver`, `rollout`, `trainer`, … |
| `turn_id`, `env_step_id` | Rollout (optional) | Multi-turn agentic rollouts |

**Cross-process linking:** parent spans on the driver use `rollout.step` / `rl.step`; child workers share `rollout_id` + `step_id` and set `actor_role`. Propagate with `rl.export_context()` / `rl.import_context()`.

---

## Web UI

| Page | Route | Purpose |
|------|-------|---------|
| Rollout | `/rollout` | Per-trajectory phase timeline (default) |
| Train | `/train` | Per-batch training phase timeline |
| Spans | `/spans` | Nested span tree with cross-process links |
| Process Timeline | `/process-timeline` | Per-process Gantt chart |
| Perfetto | `/perfetto` | Chrome trace export for loaded spans |

**Rollout view questions it answers:**

- Which samples made this rollout slow?
- Is latency dominated by inference, env, tool, or reward?
- Do certain prompt groups create tail trajectories?
- Do training phases overlap rollout generation as expected?

---

## Advanced Usage (Probing Core)

Probing-RL builds on the full Probing runtime. These capabilities apply to RL jobs and are useful when you need to go deeper than sample timelines.

### CLI

```bash
# Inject into running process (Linux)
probing -t <pid> inject

# Real-time stack trace
probing -t <pid> backtrace

# SQL on span / trace tables
probing -t <pid> query "SELECT name, kind, attributes FROM python.trace_event WHERE record_type = 'span_start' ORDER BY time DESC LIMIT 20;"

# Live Python REPL in target process
probing -t <pid> repl

# Memory overview
probing -t <pid> memory

# List injected processes
probing list
```

### SQL Analytics

```bash
# Phase duration aggregation (custom SQL over exported span data)
probing -t <pid> query "
  SELECT operation_name, avg(duration_ms), count(*)
  FROM profiling_data
  WHERE timestamp > now() - interval '5 minutes'
  GROUP BY operation_name
  ORDER BY avg(duration_ms) DESC
"

# Memory growth
probing -t <pid> query "
  SELECT function_name, sum(allocated_bytes) as total_alloc
  FROM memory_allocations
  WHERE timestamp > now() - interval '1 hour'
  GROUP BY function_name
  ORDER BY total_alloc DESC
"
```

### Interactive REPL

Connect to a running rollout worker or trainer without stopping the job:

```bash
probing -t <pid> repl
# remote: probing -t <host:port> repl
```

```python
>>> import torch
>>> models = [m for m in gc.get_objects() if isinstance(m, torch.nn.Module)]
```

### Distributed Training

```bash
probing cluster attach

probing -t <pid> query "SELECT src_rank, dst_rank, avg(latency_ms) FROM comm_metrics"
probing -t <pid> query "SELECT avg(gpu_util) FROM gpu_metrics WHERE timestamp > now() - 60"
```

### Dynamic Configuration

```bash
export PROBING_SAMPLE_RATE=0.1
export PROBING_RETENTION_DAYS=7

probing -t <pid> config probing.sample_rate=0.05
probing -t <pid> config probing.max_memory=1GB
```

### Lower-Level Trace Views

When sample timelines are not enough:

- **Spans** — nested spans inside one process
- **Process Timeline** — process-level timing
- **Perfetto / Chrome Tracing** — kernel-level timeline export
- **RDMA flow analysis** — `probing -t <pid> rdma`

---

## Architecture (Agent Runtime Placement)

```text
┌─────────────────────────────────────────────────────────────┐
│                    Agent RL Training Job                     │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐  │
│  │  Driver  │   │ Rollout  │   │ Sandbox  │   │ Trainer  │  │
│  │ / Router │──▶│ Workers  │──▶│ / Tools  │   │ Workers  │  │
│  └────┬─────┘   └────┬─────┘   └────┬─────┘   └────┬─────┘  │
│       │              │              │              │         │
│       └──────────────┴──────────────┴──────────────┘         │
│                         │ probing.rl spans                    │
│                         ▼                                     │
│              ┌─────────────────────┐                          │
│              │  Probing Runtime    │  inject / PROBING=1     │
│              │  spans + SQL store  │                          │
│              └──────────┬──────────┘                          │
└─────────────────────────┼─────────────────────────────────────┘
                          ▼
                 ┌─────────────────┐
                 │  Probing Web UI │
                 │ Rollout / Train │
                 └─────────────────┘
```

Probing sits **beside** your RL framework as an observability sidecar: frameworks emit spans; Probing aggregates and visualizes them. No fork of Slime/veRL/XTuner required.

For a longer narrative (problem statement, comparison with Slime/veRL tooling, roadmap), see [docs/probing-RL-agent-runtime.md](docs/probing-RL-agent-runtime.md).

---

## Development

### Prerequisites

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup toolchain install nightly && rustup default nightly
rustup target add wasm32-unknown-unknown
cargo install dioxus-cli
```

### Build

```bash
git clone https://github.com/DeepLink-org/probing.git
cd probing
make                    # dev build
make ZIG=1              # release cross-build
cd web && dx build --release
make wheel && pip install dist/probing-*.whl --force-reinstall
```

### Test

```bash
cargo install cargo-nextest --locked
make test
PROBING=1 python examples/test_probing.py
bash debug/test/ray_async_rl_demo.sh
```

### Project Structure

| Path | Role |
|------|------|
| `python/probing/rl.py` | RL tracing API |
| `python/probing/ext/ray.py` | Ray / Slime integration hooks |
| `examples/ray_job_actor_span_demo.py` | Async RL demo |
| `web/` | Rollout / Train Web UI |
| `probing/core/` | Profiling engine |
| `probing/cli/` | CLI (`inject`, `query`, `repl`, …) |

Detailed RL integration reference: [readme.md](readme.md).

---

## Roadmap

- URL deep links (`?rollout_id=`) from training logs
- Rollout-level percentiles and top slow samples by phase
- Inference engine dashboards — concurrency, TPOT, in-flight requests, and related serving metrics (SGLang, vLLM, etc.)
- Trace sampling for large production runs
- First-party adapters for Slime, veRL, XTuner
- Online diagnosis hooks (timeout kill, concurrency throttle) — intervention layer on top of observation

---

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make changes and add tests
4. Run `make test`
5. Open a pull request

## License

[Apache License 2.0](LICENSE)
