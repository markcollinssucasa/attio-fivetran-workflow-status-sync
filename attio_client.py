from typing import Optional, Dict, Any, List, Type, TypeVar

import httpx
from pydantic import BaseModel

TModel = TypeVar("TModel", bound=BaseModel)


class AttioClient:
  BASE_URL = "https://api.attio.com/v2"
  """Minimal async client for Attio REST API v2.

	Features:
	- POST /v2/objects/{object}/records/query (supports generic filter/limit/offset)
	- GET  /v2/objects/{object}/records/{record_id}/attributes/{attribute}/values?show_historic=true

	Note on filtering by last updated date:
	The exact attribute/key for filtering by update time can vary by workspace
	configuration. Commonly, an attribute like "updated_at" is used with a
	comparison (e.g., gte). Use the generic "filter" parameter to pass the
	exact structure expected by your workspace.
	"""

  def __init__(
    self, *, attio_token: str, base_url: str = BASE_URL, timeout_seconds: float = 30.0
  ) -> None:
    self._base_url = base_url
    self._headers = {
      "Authorization": f"Bearer {attio_token}",
      "Content-Type": "application/json",
    }
    self._timeout = timeout_seconds
    self._client: Optional[httpx.AsyncClient] = None

  async def __aenter__(self) -> "AttioClient":
    self._client = httpx.AsyncClient(
      base_url=self._base_url, headers=self._headers, timeout=self._timeout
    )
    return self

  async def __aexit__(self, exc_type, exc, tb) -> None:
    if self._client is not None:
      await self._client.aclose()
      self._client = None

  def _ensure_client(self) -> httpx.AsyncClient:
    if self._client is None:
      self._client = httpx.AsyncClient(
        base_url=self._base_url, headers=self._headers, timeout=self._timeout
      )
    return self._client

  async def query_records(
    self,
    model: Type[TModel],
    parent_object: str,
    filter: Optional[Dict[str, Any]] = None,
    limit: int = 50,
    offset: int = 0,
    sort: Optional[List[Dict[str, Any]]] = None,
  ) -> List[TModel]:
    """Query records for an object using Attio's POST records query endpoint.

    Payload shape mirrors Attio's API. "filter" is passed through as-is.
    """
    client = self._ensure_client()
    payload: Dict[str, Any] = {
      "filter": filter or {},
      "limit": limit,
      "offset": offset,
    }
    if sort is not None:
      payload["sort"] = sort

    resp = await client.post(f"/objects/{parent_object}/records/query", json=payload)
    try:
      resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
      raise SystemExit(
        f"Attio API error (query): {exc.response.status_code} {exc.response.text}"
      )
    return [model.model_validate(item) for item in resp.json()["data"]]

  async def list_attribute_values(
    self,
    model: Type[TModel],
    parent_object: str,
    record_id: str,
    attribute: str,
    show_historic: bool = True,
  ) -> List[TModel]:
    """List values for a record attribute, optionally including historic values."""
    client = self._ensure_client()
    params = {"show_historic": "true"} if show_historic else {}
    resp = await client.get(
      f"/objects/{parent_object}/records/{record_id}/attributes/{attribute}/values",
      params=params,
    )
    try:
      resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
      raise SystemExit(
        f"Attio API error (attribute values): {exc.response.status_code} {exc.response.text}"
      )
    return [model.model_validate(item) for item in resp.json()["data"]]
