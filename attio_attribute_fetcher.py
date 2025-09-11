import asyncio
import logging
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Dict, Generic, Type, TypeVar

from pydantic import BaseModel

from attio_client import AttioClient
from attio_types.attio_application import AttioApplicationRecord

RecordT = TypeVar("RecordT", bound=AttioApplicationRecord)
AttributeT = TypeVar("AttributeT", bound=BaseModel)

logger = logging.getLogger(__name__)


@dataclass
class AttioAttributeFetcherResult[AttributeT]:
  record_id: str
  values: list[AttributeT]


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
  _input_queue: asyncio.Queue[str | None] | None
  _output_queue: asyncio.Queue[AttioAttributeFetcherResult[AttributeT] | None] | None
  _is_running: bool

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
    self._input_queue = None
    self._output_queue = None
    self._is_running = False

  async def stream_attribute_values(self) -> AsyncGenerator[AttioAttributeFetcherResult[AttributeT], None]:
    """Stream lists of attribute values as they are fetched.

    This implementation uses a simple async queue pipeline:
    - Producer: pages through records and enqueues record IDs
    - Workers: consume record IDs and fetch attribute values concurrently
    - Consumer (this generator): yields results from an output queue as ready
    """
    # Guard against concurrent runs
    if self._is_running:
      raise RuntimeError(
        "Fetcher is already running; concurrent runs are not supported"
      )
    self._is_running = True
    self._input_queue = asyncio.Queue(maxsize=self.concurrency * 4)
    self._output_queue = asyncio.Queue()

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
      # Reset state
      self._is_running = False
      self._input_queue = None
      self._output_queue = None

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
            assert self._input_queue is not None
            await self._input_queue.put(record.id.record_id)
          except Exception as err:
            logger.debug("Error enqueuing record id: %s", err)

        offset += len(records)
        if self.limit is not None and len(records) < self.limit:
          break
    finally:
      # Tell all workers to stop
      assert self._input_queue is not None
      for _ in range(self.concurrency):
        await self._input_queue.put(None)

  async def _worker(self) -> None:
    try:
      while True:
        assert self._input_queue is not None
        record_id = await self._input_queue.get()
        if record_id is None:
          break
        try:
          result = await self._fetch_attribute_values(record_id)
          if result is not None:
            assert self._output_queue is not None
            await self._output_queue.put(result)
        except Exception as err:
          logger.debug("Worker fetch error: %s", err)
    except Exception as err:
      logger.debug("Worker crashed: %s", err)
    finally:
      assert self._output_queue is not None
      await self._output_queue.put(None)

  async def _fetch_attribute_values(
    self, record_id: str
  ) -> AttioAttributeFetcherResult[AttributeT] | None:
    try:
      async with self.semaphore:
        values = await self.attio_client.list_attribute_values(
          model=self.attribute_model,
          parent_object=self.parent_object,
          record_id=record_id,
          attribute=self.attribute,
          show_historic=True,
        )
        return AttioAttributeFetcherResult(record_id=record_id, values=values)
    except Exception as err:
      logger.debug("Error fetching attribute values: %s", err)
      return None
