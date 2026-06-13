# LangGraph-event-stream
Stream LangGraph execution events in real time using FastAPI and Server-Sent Events (SSE).

# How to Use
1. Copy the stream_utils.py and node_labels.py (after making changes in the langgraph names that you use)
2. In the router.py file, there is an output_formattter function, which takes in final grapg state and puts out the output in the required format.
