"""
Hermes Memory Layer

Vector storage and semantic search for historical events, research observations,
and verified insights.

All tools depend on the VectorStoreInterface ABC — not on a specific vendor
implementation.  This allows swapping Chroma for pgvector, Qdrant, LanceDB, or
Mem0/Letta without touching any tool function.
"""

from .vector_store_interface import VectorStoreInterface

__all__ = ["VectorStoreInterface"]
