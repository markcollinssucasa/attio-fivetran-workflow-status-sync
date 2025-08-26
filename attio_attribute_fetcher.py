import asyncio
import sys
from typing import Any, AsyncGenerator, Dict, List, Type, TypeVar, Generic, Optional
from pydantic import BaseModel

from types.attio_application import AttioApplicationRecord
from attio_client import AttioClient

RecordT = TypeVar("RecordT", bound=AttioApplicationRecord)
AttributeT = TypeVar("AttributeT", bound=BaseModel)


class AttioAttributeFetcher(Generic[RecordT, AttributeT]):
  concurrency: int
  attio_client: AttioClient
  semaphore: asyncio.Semaphore
  parent_object: str
  attribute: str
  record_model: Type[RecordT]
  attribute_model: Type[AttributeT]
  limit: int
  filter: Dict[str, Any]  
  _input_queue: asyncio.Queue[str | None]
  _output_queue: asyncio.Queue[List[AttributeT] | None]

  """Concurrent attribute fetcher for Attio records.

  Maintains a shared `AttioClient` and uses a semaphore to limit concurrent
  requests. Intended for attributes like workflow status that can have multiple
  historic values per record.
  """

  def __init__(
    self,
    attio_client: AttioClient,
    concurrency: int,
    parent_object: str,
    attribute: str,
    record_model: Type[RecordT],
    attribute_model: Type[AttributeT],
    filter: Dict[str, Any],
    limit: int = 50,
  ) -> None:
    self.attio_client = attio_client
    self.concurrency = concurrency
    self.semaphore = asyncio.Semaphore(concurrency)
    self.parent_object = parent_object
    self.attribute = attribute
    self.record_model = record_model
    self.attribute_model = attribute_model
    self.limit = limit
    self.filter = filter
    self._input_queue = asyncio.Queue(maxsize=self.concurrency * 4)
    self._output_queue = asyncio.Queue()

  async def run(self) -> AsyncGenerator[List[AttributeT], None]:
    """Stream lists of attribute values as they are fetched.

    This implementation uses a simple async queue pipeline:
    - Producer: pages through records and enqueues record IDs
    - Workers: consume record IDs and fetch attribute values concurrently
    - Consumer (this generator): yields results from an output queue as ready
    """
    # Guard against concurrent runs
    if self._input_queue is not None or self._output_queue is not None:
      raise RuntimeError(
        "Fetcher is already running; concurrent runs are not supported"
      )

    num_workers = self.concurrency

    producer_task = asyncio.create_task(self._producer())
    worker_tasks = [asyncio.create_task(self._worker()) for _ in range(num_workers)]

    finished_workers = 0
    try:
      while finished_workers < num_workers:
        assert self._output_queue is not None
        item = await self._output_queue.get()
        if item is None:
          finished_workers += 1
          continue
        yield item
    finally:
      # Ensure background tasks are concluded
      try:
        await producer_task
      except Exception:
        pass
      await asyncio.gather(*worker_tasks, return_exceptions=True)

  async def _producer(self) -> None:
    offset = 0
    try:
      while True:
        records = await self.attio_client.query_records(
          model=self.record_model,
          parent_object=self.parent_object,
          filter=self.filter or {},
          limit=self.limit,
          offset=offset,
        )

        if not records:
          break

        for record in records:
          try:
            await self._input_queue.put(record.id.record_id)
          except Exception as err:
            print(f"Error enqueuing record id: {err}", file=sys.stderr)

        offset += len(records)
        if self.limit is not None and len(records) < self.limit:
          break
    finally:
      # Tell all workers to stop
      for _ in range(self.concurrency):
        await self._input_queue.put(None)

  async def _worker(self) -> None:
    try:
      while True:
        record_id = await self._input_queue.get()
        if record_id is None:
          break
        try:
          result = await self._fetch_attribute_values(record_id)
          if result is not None:
            await self._output_queue.put(result)
        except Exception as err:
          print(f"Error in worker while fetching values: {err}", file=sys.stderr)
    except Exception as err:
      print(f"Worker crashed: {err}", file=sys.stderr)
    finally:
      await self._output_queue.put(None)

  async def _fetch_attribute_values(self, record_id: str) -> List[AttributeT] | None:
    try:
      async with self.semaphore:
        values = await self.attio_client.list_attribute_values(
          model=self.attribute_model,
          parent_object=self.parent_object,
          record_id=record_id,
          attribute=self.attribute,
          show_historic=True,
        )
        return values
    except Exception as err:
      print(f"Error fetching attribute values: {err}", file=sys.stderr)
      return None
