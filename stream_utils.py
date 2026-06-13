"""
Reusable LangGraph streaming utility.
Drop this into any project — just supply a compiled graph and a node-label map.
"""

import json
import asyncio
import logging
from typing import AsyncIterator, Any, Callable, Optional
# from langgraph.graph.graph import CompiledGraph

logger = logging.getLogger(__name__)


def _make_event(event_type: str, payload: dict) -> str:
    """Serialize a single SSE frame."""
    data = json.dumps({"type": event_type, **payload})
    return f"data: {data}\n\n"


async def stream_graph_events(
    graph,
    initial_state: dict,
    node_labels: dict[str, str],
    *,
    include_final_output: bool = True,
    output_formatter: Optional[Callable[[dict], dict]] = None,
) -> AsyncIterator[str]:
    """
    Stream LangGraph execution as SSE frames.

    Yields one SSE string per event. Consumers forward these directly
    to the HTTP response.

    Event types emitted
    -------------------
    node_start   – a node is about to run          { node, label }
    node_end     – a node finished                 { node, label, output_keys }
    final_output – the full graph result           { result }
    error        – something went wrong            { message }

    Parameters
    ----------
    graph            : compiled LangGraph workflow
    initial_state    : dict passed to graph.astream_events
    node_labels      : { "node_name": "Human-readable label" }
    include_final_output : whether to emit a final_output event
    output_formatter : optional fn(raw_state) -> serialisable dict
    """
    try:
        async for event in graph.astream_events(initial_state, version="v2"):
            kind = event.get("event")
            name = event.get("name", "")
            tags = event.get("tags", [])

            # ── node starting ───────────────────────────────────────────────
            if kind == "on_chain_start" and name in node_labels:
                label = node_labels[name]
                logger.debug("node_start: %s (%s)", name, label)
                yield _make_event("node_start", {"node": name, "label": label})

            # ── node finished ───────────────────────────────────────────────
            elif kind == "on_chain_end" and name in node_labels:
                label = node_labels[name]
                output = event.get("data", {}).get("output", {})
                output_keys = list(output.keys()) if isinstance(output, dict) else []
                logger.debug("node_end: %s, keys=%s", name, output_keys)
                yield _make_event(
                    "node_end",
                    {"node": name, "label": label, "output_keys": output_keys},
                )
            
            elif (
                include_final_output
                and kind == "on_chain_end"
                and name not in node_labels
                and "graph_node" not in tags
                and isinstance(event.get("data", {}).get("output"), dict)
                and event["data"]["output"]  # non-empty
            ):
                final_state = event["data"]["output"]
                logger.debug("graph finished, final keys: %s", list(final_state.keys()))
                result = output_formatter(final_state) if output_formatter else final_state
                safe_result = _safe_serialise(result)
                yield _make_event("final_output", {"result": safe_result})

    except asyncio.CancelledError:
        logger.info("Stream cancelled by client.")
    except Exception as exc:
        logger.exception("Streaming error: %s", exc)
        yield _make_event("error", {"message": str(exc)})


def _safe_serialise(obj: Any) -> Any:
    """
    Recursively convert an object to something json.dumps can handle.
    Pydantic models → .model_dump(), everything else → str().
    """
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, dict):
        return {k: _safe_serialise(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_safe_serialise(i) for i in obj]
    # Pydantic v2
    if hasattr(obj, "model_dump"):
        return _safe_serialise(obj.model_dump())
    # Pydantic v1
    if hasattr(obj, "dict"):
        return _safe_serialise(obj.dict())
    return str(obj)