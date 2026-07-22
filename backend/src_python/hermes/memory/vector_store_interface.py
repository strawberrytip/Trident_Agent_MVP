"""
Hermes Memory — VectorStoreInterface (ABC)

Abstract base class defining the contract for vector similarity search and store.

Concrete implementations:
  - ChromaVectorStore  (embedded, local persistence — MVP default)
  - MemoryVectorStore  (in-memory, for unit tests)
  - Mem0VectorStore    (future: managed memory with auto-evolution)
  - LettaVectorStore   (future: stateful agent memory with write-back)

Design principle:  Tool functions and the agent runtime depend ONLY on this
interface.  The concrete store is injected at construction time.  This means
swapping the vector backend requires exactly one line of code change (the
constructor argument) — zero code changes in tools/ or hermes_runtime.py.

Graceful degradation:  If the concrete store is unavailable (e.g. chromadb not
installed), tools that depend on vector search return empty results rather than
crashing.  The `is_available()` method lets callers check before invoking.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class VectorStoreInterface(ABC):
    """Abstract interface for vector similarity search and storage.

    All methods are async to avoid blocking the event loop, even though some
    backends (Chroma) are synchronous under the hood.  Implementations should
    wrap sync calls with `asyncio.to_thread()` where needed.
    """

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @abstractmethod
    async def initialize(self) -> None:
        """Set up the store: create collections, load indices, verify connectivity.

        Called once at HermesAgent startup.  May be a no-op for embedded stores.
        """
        ...

    @abstractmethod
    async def is_available(self) -> bool:
        """Return True if the store is operational.

        Used by tools for graceful degradation: if the store is down, vector
        search returns empty results instead of raising.
        """
        ...

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    @abstractmethod
    async def search(
        self,
        collection: str,
        query_vec: List[float],
        n_results: int = 10,
        where: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Search for nearest neighbors in a collection.

        Args:
            collection:  Collection name (e.g. "research_observations").
            query_vec:   Query embedding vector.
            n_results:   Max number of results to return.
            where:       Optional metadata filter dict (backend-specific format).

        Returns:
            List of dicts, each with keys:
              - id:              str  — document/vector id
              - document:        str  — the stored text
              - metadata:        dict — stored metadata
              - similarity:      float — cosine distance or similarity (0.0–1.0)
              - collection:      str  — collection name
        """
        ...

    # ------------------------------------------------------------------
    # Write (upsert)
    # ------------------------------------------------------------------

    @abstractmethod
    async def add(
        self,
        collection: str,
        ids: List[str],
        embeddings: List[List[float]],
        documents: List[str],
        metadatas: Optional[List[Dict[str, Any]]] = None,
    ) -> int:
        """Add or update vectors in a collection.  Returns count of items added.

        Args:
            collection:  Collection name.
            ids:         Unique identifiers (one per document).
            embeddings:  Embedding vectors, same length as ids.
            documents:   Text content, same length as ids.
            metadatas:   Optional metadata dicts, same length as ids.
        """
        ...

    # ------------------------------------------------------------------
    # Delete
    # ------------------------------------------------------------------

    @abstractmethod
    async def delete(
        self,
        collection: str,
        ids: List[str],
    ) -> int:
        """Remove vectors by id.  Returns count of items actually deleted."""
        ...

    # ------------------------------------------------------------------
    # Collection management
    # ------------------------------------------------------------------

    @abstractmethod
    async def count(self, collection: str) -> int:
        """Return the number of vectors in a collection."""
        ...

    @abstractmethod
    async def list_collections(self) -> List[str]:
        """Return names of all available collections."""
        ...

    @abstractmethod
    async def delete_collection(self, collection: str) -> bool:
        """Drop an entire collection.  Returns True if it existed."""
        ...


# ============================================================================
# In-memory reference implementation (for tests and fallback)
# ============================================================================

class MemoryVectorStore(VectorStoreInterface):
    """Brute-force in-memory vector store for unit tests and graceful degradation.

    Uses cosine similarity with O(n) search — suitable for <10k vectors.
    Not suitable for production (no persistence, no index acceleration).
    """

    def __init__(self):
        self._collections: Dict[str, List[Dict[str, Any]]] = {}
        self._available = True

    async def initialize(self) -> None:
        pass  # no-op

    async def is_available(self) -> bool:
        return self._available

    def set_available(self, available: bool) -> None:
        """Allow tests to simulate store outage."""
        self._available = available

    @staticmethod
    def _cosine_sim(a: List[float], b: List[float]) -> float:
        """Cosine similarity between two float vectors."""
        dot = sum(x * y for x, y in zip(a, b))
        norm_a = sum(x * x for x in a) ** 0.5
        norm_b = sum(x * x for x in b) ** 0.5
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return dot / (norm_a * norm_b)

    async def search(
        self,
        collection: str,
        query_vec: List[float],
        n_results: int = 10,
        where: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        if not self._available or collection not in self._collections:
            return []

        entries = self._collections[collection]
        scored: List[tuple] = []
        for entry in entries:
            # Apply metadata filter if provided
            if where:
                meta = entry.get("metadata", {})
                match = all(meta.get(k) == v for k, v in where.items())
                if not match:
                    continue
            sim = self._cosine_sim(query_vec, entry["embedding"])
            scored.append((sim, entry))

        scored.sort(key=lambda x: x[0], reverse=True)
        top = scored[:n_results]
        return [
            {
                "id": entry["id"],
                "document": entry["document"],
                "metadata": entry.get("metadata", {}),
                "similarity": round(sim, 6),
                "collection": collection,
            }
            for sim, entry in top
        ]

    async def add(
        self,
        collection: str,
        ids: List[str],
        embeddings: List[List[float]],
        documents: List[str],
        metadatas: Optional[List[Dict[str, Any]]] = None,
    ) -> int:
        if collection not in self._collections:
            self._collections[collection] = []

        if metadatas is None:
            metadatas = [{}] * len(ids)

        added = 0
        existing_ids = {e["id"] for e in self._collections[collection]}
        for i, doc_id in enumerate(ids):
            entry = {
                "id": doc_id,
                "embedding": embeddings[i],
                "document": documents[i],
                "metadata": metadatas[i] if i < len(metadatas) else {},
            }
            if doc_id in existing_ids:
                # Upsert: replace
                for j, e in enumerate(self._collections[collection]):
                    if e["id"] == doc_id:
                        self._collections[collection][j] = entry
                        break
            else:
                self._collections[collection].append(entry)
                existing_ids.add(doc_id)
            added += 1
        return added

    async def delete(self, collection: str, ids: List[str]) -> int:
        if collection not in self._collections:
            return 0
        before = len(self._collections[collection])
        ids_set = set(ids)
        self._collections[collection] = [
            e for e in self._collections[collection] if e["id"] not in ids_set
        ]
        return before - len(self._collections[collection])

    async def count(self, collection: str) -> int:
        return len(self._collections.get(collection, []))

    async def list_collections(self) -> List[str]:
        return list(self._collections.keys())

    async def delete_collection(self, collection: str) -> bool:
        existed = collection in self._collections
        self._collections.pop(collection, None)
        return existed
