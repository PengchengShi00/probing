"""Inference engine metrics for Probing-RL.

Slime exposes aggregated SGLang Prometheus metrics at ``{router_addr}/engine_metrics``.
``sglang.launch_server`` typically exposes ``{router_addr}/metrics`` when
``--enable-metrics`` is set. Use ``metrics_path`` or ``PROBING_ENGINE_METRICS_PATH``
when registering an engine.
"""

from probing.ext.engines.registry import (
    get_engine,
    list_engines,
    register_engine,
    register_slime_sglang_router,
    unregister_engine,
)
from probing.ext.engines.scraper import ensure_scraper_running, scrape_all, scrape_engine

__all__ = [
    "ensure_scraper_running",
    "get_engine",
    "list_engines",
    "register_engine",
    "register_slime_sglang_router",
    "scrape_all",
    "scrape_engine",
    "unregister_engine",
]
