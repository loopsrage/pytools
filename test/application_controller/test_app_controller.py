import base64
import unittest
from fastapi import APIRouter
from starlette.testclient import TestClient

from application_controller.app_controller import UvicornSettings, WebApp, app_registry


class TestAppController(unittest.TestCase):

    def test_webapp_middlewares_and_routes(self):
        settings = UvicornSettings(host="127.0.0.1", port=8000)
        webapp = WebApp(settings)
        webapp.async_lifespan()

        router = APIRouter()

        @router.get("/ping")
        async def ping():
            return {"pong": True}

        webapp.register_router(router)
        assert any(r.path == "/ping" for r in webapp.api_routes())

        encoded_claims = base64.b64encode(b'{"sub":"user123"}').decode("utf-8")
        with TestClient(webapp._fast_api) as client:
            resp = client.get(
                "/ping",
                headers={
                    "X-MS-CLIENT-PRINCIPAL-NAME": "alice@example.com",
                    "X-MS-CLIENT-PRINCIPAL": encoded_claims,
                },
            )
            assert resp.status_code == 200
            assert resp.json() == {"pong": True}
            assert "X-Request-Id" in resp.headers
            assert "X-Process-Time" in resp.headers

            # Also verify request without Azure headers succeeds
            resp2 = client.get("/ping")
            assert resp2.status_code == 200

        app_registry().register_applications({"test_web": webapp})
        loaded, was_loaded = app_registry().application("test_web")
        assert loaded is webapp and was_loaded is True


if __name__ == "__main__":
    unittest.main()
