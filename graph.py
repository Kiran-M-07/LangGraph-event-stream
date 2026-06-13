from typing import TypedDict, List, Optional
from langgraph.graph import StateGraph, START, END
from src.agents.router_agent import router_agent
from src.agents.note_classification_agent import note_classification_agent
from src.agents.note_cleaning_agent import note_cleaning_agent
from src.agents.note_categorization_agent import note_categorization_agent
from src.agents.signal_extraction import signal_extraction_agent
from src.agents.evidence_search import evidence_search
from src.agents.gap_detector import gap_detector
from src.api.schema.state import GraphState

def router_intent(state: GraphState):
    if state["router_response"] != "MEDICAL_NOTE":
        return END
    else:
        return "classification"
    
def note_class_check(state: GraphState):
    if state["is_actionable"]:
        return "note_cleaning"
    else:
        END

def create_graph():
    workflow = StateGraph(GraphState)
    workflow.add_node("router_agent", router_agent)
    workflow.add_node("note_classification_agent", note_classification_agent)
    workflow.add_node("note_cleaning_agent", note_cleaning_agent)
    workflow.add_node("note_categorization_agent", note_categorization_agent)
    workflow.add_node("signal_extraction_agent", signal_extraction_agent)
    workflow.add_node("evidence_search", evidence_search)
    workflow.add_node("gap_detector", gap_detector)

    workflow.set_entry_point("router_agent")
    workflow.add_conditional_edges(
        "router_agent",
        router_intent,
        {
            "classification":"note_classification_agent",
            END: END
        }
    )
    workflow.add_conditional_edges(
        "note_classification_agent",
        note_class_check,
        {
            "note_cleaning": "note_cleaning_agent",
            END: END
        }
    )
    
    workflow.add_edge("note_cleaning_agent", "note_categorization_agent")
    workflow.add_edge("note_categorization_agent", "signal_extraction_agent")
    workflow.add_edge("signal_extraction_agent","evidence_search")
    # workflow.add_edge("evidence_search", END)
    workflow.add_edge("evidence_search", "gap_detector")
    workflow.add_edge("gap_detector", END)

    return workflow.compile()
