from typing import List

from langgraph.store.postgres.base import PostgresIndexConfig

from src.thread_safe.index import Index

_idx = Index()

class PGS:

    _store: AsyncPostgresStore = None

    async def ainit(self, pool, dimensions: int, embedding, fields: List[str], index_kwargs):
        self._store, was_loaded = _idx.load_or_store_in_index(str(dimensions), dimensions, AsyncPostgresStore(
            conn=pool,
            index=PostgresIndexConfig(dims=dimensions, embed=embedding, fields=fields, **index_kwargs),
        ))

        if not was_loaded:
            await self._store.setup()

    async def asearch(self, user_id, namespace: str, query):
        return await self._store.asearch((user_id, namespace), query=query)

    async def aput(self, user_id, request_id, namespace: str, text):
        await self._store.aput((user_id, namespace), key=request_id, value=text)

    @property
    def store(self):
        return self._store

