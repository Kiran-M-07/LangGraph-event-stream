from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from src.graph import create_graph, GraphState
from src.api.schema.data_models import ChatRequest, SignalExtraction
import logging
from src.utils.stream_utils import stream_graph_events
from src.utils.node_labels import NODE_LABELS

logger = logging.getLogger(__name__)

graph = create_graph()

router = APIRouter(tags=["Gap_Identification"])

def _format_output(state: GraphState) -> dict:
    """
    Convert the raw LangGraph state into the API response shape.
    Used by both the streaming and blocking endpoints.
    """
    return {
        "cleaned_note":        state.get("cleaned_note"),
        "tags":                state.get("tags"),
        "signal_extraction":   SignalExtraction(signals=state["signal_extraction"]),
        "evidence_generation": state.get("top_evidences"),
        "gap_identified":      state.get("gap_identified"),
    }
    
@router.post("/process-note-stream")
async def process_msl_note_stream(request: ChatRequest):
    """
    Stream the processing pipeline as Server-Sent Events.
 
    Each SSE frame is a JSON object with a `type` field:
 
    ```
    data: {"type": "node_start",   "node": "...", "label": "🧹 Cleaning note..."}
    data: {"type": "node_end",     "node": "...", "label": "...", "output_keys": [...]}
    data: {"type": "final_output", "result": { ... }}
    data: {"type": "error",        "message": "..."}
    ```
    """
    initial_state: GraphState = {
        "raw_note": request.messages[0].content,
    }
 
    return StreamingResponse(
        stream_graph_events(
            graph,
            initial_state,
            NODE_LABELS,
            include_final_output=True,
            output_formatter=_format_output,
        ),
        media_type="text/event-stream",
        headers={
            # Prevent proxies / Nginx from buffering the stream
            "Cache-Control":    "no-cache",
            "X-Accel-Buffering": "no",
        },
    )