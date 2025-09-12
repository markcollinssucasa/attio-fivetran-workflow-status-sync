from __future__ import annotations

from typing import Optional, Dict, Any, List, Type, TypeVar
import httpx
from httpx_retries import RetryTransport, Retry
from aiolimiter import AsyncLimiter
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

  _base_url: str
  _headers: Dict[str, str]
  _timeout: httpx.Timeout
  _rate_limit: AsyncLimiter
  _client: Optional[httpx.AsyncClient]
  _retry: Retry
  _transport: RetryTransport

  def __init__(
    self,
    attio_token: str,
    base_url: str = BASE_URL,
    timeout_seconds: float = 30.0,
    rate_limit_per_window: int = 100,
    rate_limit_window_seconds: int = 30,
    max_retries: int = 3,
    backoff_base_seconds: float = 0.5,
  ) -> None:
    self._base_url = base_url.rstrip("/")
    self._headers = {
      "Authorization": f"Bearer {attio_token}",
      "Content-Type": "application/json",
    }
    self._timeout = httpx.Timeout(timeout_seconds)  # can swap for granular if needed
    self._rate_limit = AsyncLimiter(rate_limit_per_window, rate_limit_window_seconds)
    self._client = None
    # Configure retries via httpx-retries
    self._retry = Retry(
      total=max_retries,
      backoff_factor=backoff_base_seconds,
      backoff_jitter=0.1,
      status_forcelist=[429, 500, 502, 503, 504],
      allowed_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
    )
    self._transport = RetryTransport(retry=self._retry)

  async def __aenter__(self) -> "AttioClient":
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
    if self._client is None:
      self._client = httpx.AsyncClient(
        base_url=self._base_url,
        headers=self._headers,
        timeout=self._timeout,
        transport=self._transport,
      )
    return self._client

  async def _request_with_rate_limit(
    self, method: str, url: str, **kwargs
  ) -> httpx.Response:
    """
    Send an HTTP request under the shared AsyncLimiter.

    Notes:
    - Rate limiting is enforced via AsyncLimiter to smooth bursts and cap request rate.
    - Retries and backoff for 429/5xx are handled by the configured RetryTransport.
    """
    client = self._ensure_client()
    async with self._rate_limit:
      return await client.request(method, url, **kwargs)

  async def query_records(
    self,
    model: Type[TModel],
    parent_object: str,
    filter: Optional[Dict[str, Any]] = None,
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
      "filter": filter or {},
      "limit": limit,
      "offset": offset,
    }
    if sort is not None:
      payload["sort"] = sort

    resp = await self._request_with_rate_limit(
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
    resp = await self._request_with_rate_limit(
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
    filter: Optional[Dict[str, Any]] = None,
    sort: Optional[List[Dict[str, Any]]] = None,
    page_size: int = 200,
  ):
    offset = 0
    while True:
      page = await self.query_records(
        model=model,
        parent_object=parent_object,
        filter=filter,
        limit=page_size,
        offset=offset,
        sort=sort,
      )
      if not page:
        break
      for item in page:
        yield item
      offset += len(page)
