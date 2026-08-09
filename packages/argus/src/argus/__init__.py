"""argus — every LLM call, watched.

Auto-instrumenting observability SDK for LLM inference.

Target usage (implemented in P2):

    import argus
    argus.init(endpoint="http://ingestion:8001/v1/events", service="chat-app")

    with argus.conversation(conversation_id):
        resp = client.chat.completions.create(...)   # logged, no call-site change
"""

__version__ = "0.1.0"
