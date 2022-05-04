from typing import Any, Callable
import os

import httpx


def _get_client():
    return httpx.Client(
        base_url="https://rest.gohighlevel.com/v1/",
        headers={"Authorization": f"Bearer {os.getenv('REDUP_API_KEY')}"},
    )


def get_listing(
    uri: str,
    res_fn: Callable[[dict[str, Any]], list[dict[str, Any]]],
    cursor_fn: Callable[[dict[str, Any]], dict[str, Any]],
):
    def _get(params: dict[str, str]):
        def __get(_params: dict[str, Any] = {}) -> list[dict[str, Any]]:
            r = client.get(uri, params={**params, **_params})
            res = r.json()
            data = res_fn(res)
            return data if not data else data + __get(cursor_fn(res))

        with _get_client() as client:
            return __get()

    return _get


def get_dimensions(uri: str, res_fn: Callable[[dict[str, Any]], list[dict[str, Any]]]):
    def _get(*args) -> list[dict[str, Any]]:
        with _get_client() as client:
            r = client.get(uri)
            res = r.json()
            return res_fn(res)

    return _get
