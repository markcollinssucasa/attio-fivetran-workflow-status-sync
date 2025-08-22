import asyncio
import sys
from typing import Any, AsyncGenerator, Dict, List, Type, TypeVar, Generic
from pydantic import BaseModel

from attio_application import AttioRecord
from atttio_client import AttioClient

RecordT = TypeVar("RecordT", bound=AttioRecord)
AttributeT = TypeVar("AttributeT", bound=BaseModel)


class AttioWorkflowStatusFetcher(Generic[RecordT, AttributeT]):
  concurrency: int
  attio_client: AttioClient
  semaphore: asyncio.Semaphore

  """Concurrent attribute fetcher for Attio records.

  Maintains a shared `AttioClient` and uses a semaphore to limit concurrent
  requests. Intended for attributes like workflow status that can have multiple
  historic values per record.
  """

  def __init__(self, *, attio_client: AttioClient, concurrency: int) -> None:
    self.attio_client = attio_client
    self.concurrency = concurrency
    self.semaphore = asyncio.Semaphore(concurrency)

  async def run(
    self,
    parent_object: str,
    attribute: str,
    record_model: Type[RecordT],
    attribute_model: Type[AttributeT],
    updated_after_iso: str,
    limit: int,
    filter: Dict[str, Any],
  ) -> AsyncGenerator[List[AttributeT], None]:
    """Stream lists of attribute values for each record page.

    Parameters left as-is to preserve public API. `updated_after_iso` is not
    used directly here; include any update-time filtering in `filter`.
    """
    offset = 0
    while True:
      records = await self.attio_client.query_records(
        model=record_model,
        parent_object=parent_object,
        filter=filter,
        limit=limit,
        offset=offset,
      )

      if not records:
        break

      record_ids: List[str] = [record.id.record_id for record in records]

      tasks: List[asyncio.Task[List[AttributeT] | None]] = []

      async with asyncio.TaskGroup() as task_group:
        for rid in record_ids:
          task = task_group.create_task(
            self._fetch_attribute_values(rid, attribute_model, parent_object, attribute)
          )
          tasks.append(task)

      for task in tasks:
        try:
          result = task.result()
          if result is None:
            continue
          yield result
        except Exception as err:
          print(f"Error fetching attribute values: {err}", file=sys.stderr)
          continue

      offset += len(records)

      if len(records) < limit:
        break

  async def _fetch_attribute_values(
    self,
    record_id: str,
    attribute_model: Type[AttributeT],
    parent_object: str,
    attribute: str,
  ) -> List[AttributeT] | None:
    try:
      async with self.semaphore:
        values = await self.attio_client.list_attribute_values(
          model=attribute_model,
          parent_object=parent_object,
          record_id=record_id,
          attribute=attribute,
          show_historic=True,
        )
        return values
    except Exception as err:
      print(f"Error fetching attribute values: {err}", file=sys.stderr)
      return None
