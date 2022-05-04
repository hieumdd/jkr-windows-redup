from typing import Any, Callable
from dataclasses import dataclass


@dataclass
class Pipeline:
    name: str
    params_fn: Callable[[dict[str, Any]], dict[str, Any]]
    get: Callable[[Any], list[dict[str, Any]]]
    transform: Callable[[list[dict[str, Any]]], list[dict[str, Any]]]
    schema: list[dict[str, Any]]
    cursor_key: str
    id_key: str = "id"
