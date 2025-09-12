from __future__ import annotations

from typing import Optional, Dict, Any, List, Type, TypeVar

import asyncio
import httpx
from pydantic import BaseModel, ValidationError

TModel = TypeVar("TModel", bound=BaseModel)


class AttioAPIError(Exception):
  """Raised for non-2xx Attio responses with useful context."""

  def __init__(self, message: str, status_code: int, body: str):
    super().__init__(message)
    self.status_code = status_code
    self.body = body


class AttioClient:
  """
  Minimal async client for Attio REST API v2.

  Features:
  - POST /v2/objects/{object}/records/query
  - GET  /v2/objects/{object}/records/{record_id}/attributes/{attribute}/values?show_historic=true

  Notes:
  - Use `query_filter` for Attio's filter payload (workspace-specific schema).
  - Provides basic retry/backoff for 429/5xx.
  """

  BASE_URL = "https://api.attio.com/v2"

  def __init__(
    self,
    *,
    attio_token: str,
    base_url: str = BASE_URL,
    timeout_seconds: float = 30.0,
    max_retries: int = 3,
    backoff_base: float = 0.5,  # seconds
  ) -> None:
    self._base_url = base_url.rstrip("/")
    self._headers = {
      "Authorization": f"Bearer {attio_token}",
      "Content-Type": "application/json",
    }
    self._timeout = httpx.Timeout(timeout_seconds)  # can swap for granular if needed
    self._client: Optional[httpx.AsyncClient] = None
    self._transport = httpx.AsyncHTTPTransport(retries=0)  # we handle retries ourselves
    self._max_retries = max_retries
    self._backoff_base = backoff_base

  async def __aenter__(self) -> "AttioClient":
    # Prefer one lifecycle: context manager users get a single client
    self._client = httpx.AsyncClient(
      base_url=self._base_url,
      headers=self._headers,
      timeout=self._timeout,
      transport=self._transport,
    )
    return self

  async def __aexit__(self, exc_type, exc, tb) -> None:
    await self.aclose()

  async def aclose(self) -> None:
    if self._client is not None:
      await self._client.aclose()
      self._client = None

  def _ensure_client(self) -> httpx.AsyncClient:
    # Optional: you can remove this to *force* context manager usage
    if self._client is None:
      self._client = httpx.AsyncClient(
        base_url=self._base_url,
        headers=self._headers,
        timeout=self._timeout,
        transport=self._transport,
      )
    return self._client

  async def _request_with_backoff(
    self, method: str, url: str, **kwargs
  ) -> httpx.Response:
    """
    Retries on 429 and 5xx with exponential backoff.
    For 429, honors Retry-After seconds if present.
    """
    client = self._ensure_client()

    attempt = 0
    while True:
      resp = await client.request(method, url, **kwargs)
      if resp.status_code < 500 and resp.status_code != 429:
        return resp

      attempt += 1
      if attempt > self._max_retries:
        return resp

      # Compute backoff
      retry_after = resp.headers.get("Retry-After")
      if retry_after and retry_after.isdigit():
        delay = float(retry_after)
      else:
        delay = self._backoff_base * (2 ** (attempt - 1))

      await asyncio.sleep(delay)

  async def query_records(
    self,
    model: Type[TModel],
    parent_object: str,
    query_filter: Optional[Dict[str, Any]] = None,
    limit: int = 50,
    offset: int = 0,
    sort: Optional[List[Dict[str, Any]]] = None,
  ) -> List[TModel]:
    """
    Query records for an object using Attio's POST records query endpoint.

    Args:
        model: Pydantic model to validate each item in `data`.
        parent_object: Attio object slug.
        query_filter: Dict matching Attio's expected filter structure.
        limit/offset: Pagination.
        sort: Optional Attio sort payload.

    Returns:
        list[model]
    """
    payload: Dict[str, Any] = {
      "filter": query_filter or {},
      "limit": limit,
      "offset": offset,
    }
    if sort is not None:
      payload["sort"] = sort

    resp = await self._request_with_backoff(
      "POST",
      f"/objects/{parent_object}/records/query",
      json=payload,
    )
    if resp.is_error:
      raise AttioAPIError(
        f"Attio API error (query): {resp.status_code}",
        resp.status_code,
        resp.text,
      )

    try:
      data = resp.json().get("data", [])
    except ValueError:
      raise AttioAPIError(
        "Invalid JSON in response (query)", resp.status_code, resp.text
      )

    try:
      return [model.model_validate(item) for item in data]
    except ValidationError as ve:
      raise AttioAPIError(
        f"Pydantic validation failed: {ve}", resp.status_code, resp.text
      )

  async def list_attribute_values(
    self,
    model: Type[TModel],
    parent_object: str,
    record_id: str,
    attribute: str,
    show_historic: bool = True,
  ) -> List[TModel]:
    """
    List values for a record attribute, optionally including historic values.
    """
    params = {"show_historic": "true"} if show_historic else {}
    resp = await self._request_with_backoff(
      "GET",
      f"/objects/{parent_object}/records/{record_id}/attributes/{attribute}/values",
      params=params,
    )
    if resp.is_error:
      raise AttioAPIError(
        f"Attio API error (attribute values): {resp.status_code}",
        resp.status_code,
        resp.text,
      )

    try:
      data = resp.json().get("data", [])
    except ValueError:
      raise AttioAPIError(
        "Invalid JSON in response (attribute values)", resp.status_code, resp.text
      )

    try:
      return [model.model_validate(item) for item in data]
    except ValidationError as ve:
      raise AttioAPIError(
        f"Pydantic validation failed: {ve}", resp.status_code, resp.text
      )

  # Optional convenience: iterate all pages without manual offset math.
  async def iter_all_records(
    self,
    model: Type[TModel],
    parent_object: str,
    query_filter: Optional[Dict[str, Any]] = None,
    sort: Optional[List[Dict[str, Any]]] = None,
    page_size: int = 200,
  ):
    offset = 0
    while True:
      page = await self.query_records(
        model=model,
        parent_object=parent_object,
        query_filter=query_filter,
        limit=page_size,
        offset=offset,
        sort=sort,
      )
      if not page:
        break
      for item in page:
        yield item
      offset += len(page)
