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
  _concurrency: int
  _attio_client: AttioClient
  _parent_object: str
  _attribute: str
  _record_model: Type[RecordT]
  _attribute_model: Type[AttributeT]
  _limit: int
  _filter: Dict[str, Any]
  _input_queue: asyncio.Queue[str | None] | None
  _output_queue: asyncio.Queue[AttioAttributeFetcherResult[AttributeT] | None] | None
  _is_running: bool

  """Concurrent attribute fetcher for Attio records.

  Implements a simple producer/worker pipeline with asyncio queues:
  - Producer pages through records and enqueues record IDs
  - Workers fetch attribute values concurrently and enqueue results
  - The generator yields results as they arrive

  Concurrency is bounded by the number of workers; request rate is limited by
  the client's limiter.
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
    self._attio_client = attio_client
    self._concurrency = concurrency
    self._parent_object = parent_object
    self._attribute = attribute
    self._record_model = record_model
    self._attribute_model = attribute_model
    self._limit = limit
    self._filter = filter
    self._input_queue = None
    self._output_queue = None
    self._is_running = False

  async def stream_attribute_values(self) -> AsyncGenerator[AttioAttributeFetcherResult[AttributeT], None]:
    """Stream lists of attribute values as they are fetched.

    Queue-based pipeline:
    - Producer pages through records and enqueues record IDs
    - Workers fetch attribute values concurrently and enqueue results
    - Consumer yields results until all workers signal completion
    """
    if self._is_running:
      raise RuntimeError(
        "Fetcher is already running; concurrent runs are not supported"
      )
    self._is_running = True
    prefetch_factor = 4
    queue_size = self._concurrency * prefetch_factor
    if self._limit is not None:
      queue_size = max(queue_size, self._limit * 2)  # at least two page
    self._input_queue = asyncio.Queue(maxsize=queue_size)
    self._output_queue = asyncio.Queue()

    num_workers = self._concurrency

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
      # guarantee all workers are finished
      await asyncio.gather(*worker_tasks, return_exceptions=True)
      # Reset state
      self._is_running = False
      self._input_queue = None
      self._output_queue = None

  async def _producer(self) -> None:
    offset = 0
    try:
      while True:
        records = await self._attio_client.query_records(
          model=self._record_model,
          parent_object=self._parent_object,
          filter=self._filter or {},
          limit=self._limit,
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
        if self._limit is not None and len(records) < self._limit:
          break
    finally:
      # Tell all workers to stop
      assert self._input_queue is not None
      for _ in range(self._concurrency):
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
      values = await self._attio_client.list_attribute_values(
        model=self._attribute_model,
        parent_object=self._parent_object,
        record_id=record_id,
        attribute=self._attribute,
        show_historic=True,
      )
      return AttioAttributeFetcherResult(record_id=record_id, values=values)
    except Exception as err:
      logger.debug("Error fetching attribute values: %s", err)
      return None
