"""
Human-readable labels for each LangGraph node in this workflow.
 
To add a new node, just add an entry here — nothing else changes.
The key must exactly match the node name registered in graph.py.
"""
 
NODE_LABELS: dict[str, str] = {
    "router_agent":              "Routing request...",
    "note_classification_agent": "Classifying note...",
    "note_cleaning_agent":       "Cleaning note...",
    "note_categorization_agent": "Categorising note...",
    "signal_extraction_agent":   "Extracting signals...",
    "evidence_search":           "Searching for evidence...",
    "gap_detector":              "Detecting gaps...",
}
 