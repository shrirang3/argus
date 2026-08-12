"""Provider adapters.

Each adapter knows one vendor's response shape and normalises it into the fields
`InferenceEvent` expects. Everything vendor-specific is confined here, so adding
a provider never touches the emitter, the schema or the pipeline.
"""

from argus.adapters.base import Extraction, extract_from_chunk, extract_from_response

__all__ = ["Extraction", "extract_from_chunk", "extract_from_response"]
