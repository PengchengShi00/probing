"""Minimal Ray + probing timeline demo.

This file intentionally keeps the workload small and dependency-light so it can
be used to debug Ray driver/actor/task process correlation before running a real
RL or ImageNet workload.

Example:
    PYTHONPATH=/mnt/shared-storage-user/shipengcheng/Repository/probing/python \
    PROBING=1 PROBING_PORT=9922 \
    python examples/imagenet_with_span.py --num-workers 2 --num-batches 8

While the script is sleeping at the end:
    probing list
    probing -t <driver-pid> query "SELECT * FROM python.ray_process ORDER BY timestamp_ns DESC LIMIT 20;"
    probing -t <driver-pid> query "SELECT * FROM python.ray_task ORDER BY start_time_ns DESC LIMIT 20;"
    probing -t <driver-pid> query "SELECT * FROM python.ray_actor ORDER BY timestamp_ns DESC LIMIT 20;"
"""

from __future__ import annotations

import argparse
import os
import random
import time
from pathlib import Path

import ray


def _ensure_repo_python_on_path() -> str:
    """Return a PYTHONPATH that lets Ray workers import this checkout's probing."""
    repo_python = str(Path(__file__).resolve().parents[1] / "python")
    current = os.environ.get("PYTHONPATH", "")
    paths = [repo_python]
    if current:
        paths.append(current)
    pythonpath = os.pathsep.join(paths)
    os.environ["PYTHONPATH"] = pythonpath
    return pythonpath


def _drop_ray_tables() -> None:
    import probing

    for name in ("ray_process", "ray_task", "ray_actor"):
        try:
            probing.ExternalTable.drop(name)
        except Exception:
            pass


def _register_process(role: str) -> None:
    try:
        from probing.ext.ray import register_current_process

        register_current_process(role)
    except Exception:
        pass


@ray.remote
def preprocess_batch(batch_id: int, batch_size: int) -> dict:
    import probing

    _register_process("ray_task_preprocess")
    with probing.span(
        "imagenet.preprocess",
        kind="ray.task",
        batch_id=batch_id,
        batch_size=batch_size,
    ):
        time.sleep(0.02 + random.random() * 0.03)
        return {
            "batch_id": batch_id,
            "num_samples": batch_size,
            "checksum": batch_id * batch_size,
        }


@ray.remote
class ImageNetWorker:
    def __init__(self, worker_id: int):
        import probing

        self.worker_id = worker_id
        _register_process("ray_actor_imagenet_worker")
        with probing.span(
            "imagenet.worker.init",
            kind="ray.actor",
            worker_id=worker_id,
        ):
            time.sleep(0.05)

    def train_step(self, batch: dict) -> dict:
        import probing

        _register_process("ray_actor_imagenet_worker")
        with probing.span(
            "imagenet.worker.train_step",
            kind="ray.actor",
            worker_id=self.worker_id,
            batch_id=batch["batch_id"],
            num_samples=batch["num_samples"],
        ):
            time.sleep(0.04 + random.random() * 0.04)
            loss = 1.0 / (batch["batch_id"] + 1)
            return {
                "worker_id": self.worker_id,
                "batch_id": batch["batch_id"],
                "loss": loss,
            }

    def evaluate(self) -> dict:
        import probing

        _register_process("ray_actor_imagenet_worker")
        with probing.span(
            "imagenet.worker.evaluate",
            kind="ray.actor",
            worker_id=self.worker_id,
        ):
            time.sleep(0.03)
            return {"worker_id": self.worker_id, "acc1": 60.0 + self.worker_id}


def run_demo(args: argparse.Namespace) -> None:
    pythonpath = _ensure_repo_python_on_path()

    import probing
    from probing.ext.ray import get_ray_timeline, register_current_process

    if args.reset_ray_tables:
        _drop_ray_tables()

    runtime_env = {
        "env_vars": {
            "PROBING": os.environ.get("PROBING", "1"),
            "PROBING_PORT": os.environ.get("PROBING_PORT", "9922"),
            "PYTHONPATH": pythonpath,
            "SLIME_PROBING_ROLE": "ray_worker",
        }
    }

    ray.init(
        address=args.address,
        ignore_reinit_error=True,
        _tracing_startup_hook="probing.ext.ray:setup_tracing",
        runtime_env=runtime_env,
    )
    register_current_process("driver")

    print(f"driver pid: {os.getpid()}")
    try:
        ray_address = ray.get_runtime_context().gcs_address
    except Exception:
        ray_address = args.address or "local"
    print(f"ray address: {ray_address}")

    with probing.span(
        "imagenet.ray_demo",
        kind="driver",
        num_workers=args.num_workers,
        num_batches=args.num_batches,
        batch_size=args.batch_size,
    ):
        workers = [ImageNetWorker.remote(i) for i in range(args.num_workers)]

        preprocess_refs = [
            preprocess_batch.remote(batch_id, args.batch_size)
            for batch_id in range(args.num_batches)
        ]
        batches = ray.get(preprocess_refs)

        train_refs = []
        for idx, batch in enumerate(batches):
            worker = workers[idx % len(workers)]
            train_refs.append(worker.train_step.remote(batch))
        train_results = ray.get(train_refs)

        eval_results = ray.get([worker.evaluate.remote() for worker in workers])

    timeline = get_ray_timeline()
    print(f"ray timeline entries: {len(timeline)}")
    print(f"train results: {train_results[:3]}")
    print(f"eval results: {eval_results}")
    print()
    print("Useful queries:")
    print(
        f'  probing -t {os.getpid()} query "SELECT * FROM python.ray_process ORDER BY timestamp_ns DESC LIMIT 20;"'
    )
    print(
        f'  probing -t {os.getpid()} query "SELECT * FROM python.ray_task ORDER BY start_time_ns DESC LIMIT 20;"'
    )
    print(
        f'  probing -t {os.getpid()} query "SELECT * FROM python.ray_actor ORDER BY timestamp_ns DESC LIMIT 20;"'
    )
    print(
        f'  probing -t {os.getpid()} query "SELECT * FROM python.trace_event ORDER BY time DESC LIMIT 20;"'
    )

    if args.sleep_seconds > 0:
        print(f"sleeping {args.sleep_seconds}s for probing CLI inspection...")
        time.sleep(args.sleep_seconds)

    if not args.keep_ray_alive:
        ray.shutdown()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ray + probing timeline demo")
    parser.add_argument(
        "--address",
        default=None,
        help="Ray address, for example 'auto'. Omit to start local Ray.",
    )
    parser.add_argument("--num-workers", type=int, default=2)
    parser.add_argument("--num-batches", type=int, default=8)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument(
        "--sleep-seconds",
        type=int,
        default=600,
        help="Keep the driver alive so probing CLI can query it.",
    )
    parser.add_argument(
        "--reset-ray-tables",
        action="store_true",
        help="Drop ray_process/ray_task/ray_actor before running.",
    )
    parser.add_argument(
        "--keep-ray-alive",
        action="store_true",
        help="Do not call ray.shutdown() before exit.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    run_demo(parse_args())
