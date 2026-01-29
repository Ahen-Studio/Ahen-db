"""
skypydb vector store implementation for mem0.
"""

import logging
from typing import Dict, List, Optional

from pydantic import BaseModel

try:
    from skypydb.api import Vector_Client
except ImportError:
    raise ImportError("The 'skypydb' library is required. Please install it using 'pip install skypydb'.")

from mem0.vector_stores.base import VectorStoreBase

logger = logging.getLogger(__name__)


class OutputData(BaseModel):
    id: Optional[str] = None
    score: Optional[float] = None
    payload: Optional[Dict] = None


class SkypyDB(VectorStoreBase):
    """skypydb vector store for mem0."""

    def __init__(
        self,
        collection_name: str,
        client: Optional[Vector_Client] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        path: Optional[str] = None,
        embedding_model: str = "nomic-embed-text",
        ollama_base_url: str = "http://localhost:11434",
    ):
        if client:
            self.client = client
        else:
            if path is None:
                path = "./db/_generated/mem0_vector.db"

            self.client = Vector_Client(
                path=path,
                host=host,
                port=port,
                embedding_model=embedding_model,
                ollama_base_url=ollama_base_url,
            )

        self.collection_name = collection_name
        self.collection = self.create_col(collection_name)

    def _parse_output(self, data: Dict) -> List[OutputData]:
        ids = data.get("ids", [])
        distances = data.get("distances", [])
        metadatas = data.get("metadatas", [])

        if ids and isinstance(ids[0], list):
            ids = ids[0]
        if distances and isinstance(distances[0], list):
            distances = distances[0]
        if metadatas and isinstance(metadatas[0], list):
            metadatas = metadatas[0]

        results = []
        for i in range(len(ids)):
            entry = OutputData(
                id=ids[i] if i < len(ids) else None,
                score=distances[i] if distances and i < len(distances) else None,
                payload=metadatas[i] if metadatas and i < len(metadatas) else None,
            )
            results.append(entry)

        return results

    def create_col(self, name: str, embedding_fn: Optional[callable] = None):
        return self.client.get_or_create_collection(name=name)

    def insert(
        self,
        vectors: List[List[float]],
        payloads: Optional[List[Dict]] = None,
        ids: Optional[List[str]] = None,
    ):
        # Handle case where vectors might be a single vector instead of list of vectors
        if vectors and not isinstance(vectors[0], list):
            vectors = [vectors]
        
        if ids is None:
            import uuid
            ids = [str(uuid.uuid4()) for _ in vectors]
        elif isinstance(ids, str):
            ids = [ids]

        if payloads is not None and not isinstance(payloads, list):
            payloads = [payloads]

        logger.info(f"Inserting {len(vectors)} vectors into collection {self.collection_name}")
        self.collection.add(ids=ids, embeddings=vectors, metadatas=payloads)

    def search(
        self,
        query: str,
        vectors: List[List[float]],
        limit: int = 5,
        filters: Optional[Dict] = None,
    ) -> List[OutputData]:
        # Handle case where vectors might be a single vector
        if vectors and not isinstance(vectors[0], list):
            vectors = [vectors]
            
        where_clause = self._generate_where_clause(filters) if filters else None
        results = self.collection.query(
            query_embeddings=vectors,
            n_results=limit,
            where=where_clause,
        )
        return self._parse_output(results)

    def delete(self, vector_id: str):
        self.collection.delete(ids=[vector_id])

    def update(
        self,
        vector_id: str,
        vector: Optional[List[float]] = None,
        payload: Optional[Dict] = None,
    ):
        embeddings = None
        if vector is not None:
            # Ensure vector is wrapped in a list
            if not isinstance(vector[0], list):
                embeddings = [vector]
            else:
                embeddings = vector
        
        self.collection.update(
            ids=[vector_id],
            embeddings=embeddings,
            metadatas=[payload] if payload else None,
        )

    def get(self, vector_id: str) -> OutputData:
        result = self.collection.get(ids=[vector_id])
        parsed = self._parse_output(result)
        return parsed[0] if parsed else OutputData()

    def list_cols(self) -> List:
        return self.client.list_collections()

    def delete_col(self):
        self.client.delete_collection(name=self.collection_name)

    def col_info(self) -> Dict:
        return {"name": self.collection_name, "count": self.collection.count()}

    def list(self, filters: Optional[Dict] = None, limit: int = 100) -> List[OutputData]:
        where_clause = self._generate_where_clause(filters) if filters else None
        results = self.collection.get(where=where_clause, limit=limit)
        return self._parse_output(results)

    def reset(self):
        logger.warning(f"Resetting collection {self.collection_name}...")
        self.delete_col()
        self.collection = self.create_col(self.collection_name)

    @staticmethod
    def _generate_where_clause(where: Optional[Dict]) -> Optional[Dict]:
        if where is None:
            return None

        def convert_condition(key: str, value) -> Optional[Dict]:
            if value == "*":
                return None
            elif isinstance(value, dict):
                result = {}
                for op, val in value.items():
                    op_map = {"eq": "$eq", "ne": "$ne", "gt": "$gt", "gte": "$gte",
                             "lt": "$lt", "lte": "$lte", "in": "$in", "nin": "$nin"}
                    result[key] = {op_map.get(op, "$eq"): val}
                return result
            else:
                return {key: {"$eq": value}}

        processed = []
        for key, value in where.items():
            if key == "$or":
                or_conds = []
                for cond in value:
                    or_cond = {}
                    for sk, sv in cond.items():
                        conv = convert_condition(sk, sv)
                        if conv:
                            or_cond.update(conv)
                    if or_cond:
                        or_conds.append(or_cond)
                if len(or_conds) > 1:
                    processed.append({"$or": or_conds})
                elif or_conds:
                    processed.append(or_conds[0])
            elif key != "$not":
                conv = convert_condition(key, value)
                if conv:
                    processed.append(conv)

        if not processed:
            return None
        elif len(processed) == 1:
            return processed[0]
        else:
            return {"$and": processed}
