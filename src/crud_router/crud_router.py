import json
import traceback
import uuid
from typing import Any, Optional

from application_controller.app_controller import App, AppIndex
from fastapi import Depends, Body
from fastapi_restful.cbv import cbv
from fsspecc.base_fsspecfs.base_fsspecfs import FSBase
from postgreslib.count_column import count_column
from postgreslib.datagrid_adapter import mui_datagrid_select_many
from postgreslib.engine import named_session
from postgreslib.upsert import upsert_entry
from postgreslib.util import to_dict
from pydantic import BaseModel
from sqlalchemy import select
from starlette.requests import Request


def select_by_id(session, model, id):
    stmt = select(model).where(model.id == id)
    return session.execute(stmt).scalar_one_or_none()

def delete_by_id(session, model, id):
    obj = session.get(model, id)
    if obj:
        session.delete(obj)
        session.commit()
        return True
    return False

async def get_app_index(request: Request) -> AppIndex:
    return request.app.state.app_index

class GridRequest(BaseModel):
    offset: int = 0
    limit: int = 100
    sortModel: Optional[str] = None
    filterModel: Optional[str] = None
    search: Optional[str] = None

def count_wrapper(named_sess: str, model):

    async def get_count_column(column_name: str = None):
        if column_name is None:
            column_name = "stage"

        with named_session(named_sess) as session:
            results = count_column(session, model, column_name)
            dat = [{"id": uuid.uuid4().hex, "name": str(row[0]), "count": row[1]} for index, row in enumerate(results)]
            return {"results": dat,
                    "pagination": {"total": len(dat)}}

    return get_count_column


def grid_wrapper(named_sess: str, model):
    async def list_training_data(request: GridRequest):
        parsed_filter = json.loads(request.filterModel) if request.filterModel else {}
        parsed_sort = json.loads(request.sortModel) if request.sortModel else []

        results = None
        with named_session(named_sess) as session:
            results = mui_datagrid_select_many(
                session,
                model,
                filter_model=parsed_filter,
                sort_model=parsed_sort,
                search=request.search,
                offset=request.offset,
                limit=request.limit,
                search_vector=model.vector()
            )

        return {"results": [to_dict(r) for r in results["rows"]],
                "pagination": {"total": results["total"]}}

    return list_training_data


class BaseApi:

    app_index: AppIndex = Depends(get_app_index)

    @property
    def fs(self) -> FSBase:
        f, _ = self.app_index.filesystem("blob")
        return f

    @property
    def app(self) -> App:
        a, _ = self.app_index.application("app")
        return a

def create_crud_router(router, named_sess, model, schema_model, index_elements):
    @cbv(router)
    class CrudRouter(BaseApi):
        @router.get("/grid")
        async def grid(self, request: GridRequest = Depends()):
            return await grid_wrapper(named_sess, model)(request)

        @router.get("/count")
        async def count(self, column: str = None):
            return await count_wrapper(named_sess, model)(column)

        @router.post("/create", response_model=None)
        async def create(self, data: schema_model):
            try:
                with named_session(named_sess) as session:
                    upsert_entry(session, model, index_elements=index_elements, **data.model_dump())
                return {"result": "success"}
            except Exception as e:
                traceback.print_exception(e)
                return {"error": str(e)}

        @router.get("/create", response_model=None)
        async def schema(self):
            return schema_model.model_construct()

        @router.get("/get")
        async def get(self, id: Any):
            try:
                with named_session(named_sess) as session:
                    result = select_by_id(session, model, id)
                return result
            except Exception as e:
                traceback.print_exception(e)
                return {"error": str(e)}

        @router.get("/delete")
        async def delete(self, id: Any):
            try:
                with named_session(named_sess) as session:
                    result = delete_by_id(session, model, id)
                return {"success": result}
            except Exception as e:
                traceback.print_exception(e)
                return {"error": str(e)}

    return router


