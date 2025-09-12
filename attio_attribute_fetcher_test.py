import os
import json
from datetime import datetime, timedelta

import pytest

from attio_client import AttioClient
from attio_types.attio_application import AttioApplicationRecord
from attio_types.attio_application_workflow_status_atttribute import (
  AttioApplicationWorkflowStatusAttribute,
)
from attio_attribute_fetcher import AttioAttributeFetcher
from settings import Settings
from helpers import convert_to_iso


PARENT_OBJECT = os.getenv("ATTIO_TEST_PARENT_OBJECT", "applications")
ATTRIBUTE = os.getenv("ATTIO_TEST_ATTRIBUTE", "workflow_status")


def _require_token() -> str:
  settings = Settings()
  if not settings.ATTIO_API_TOKEN:
    pytest.skip("Missing ATTIO_API_TOKEN in environment. Set it in your .env file.")
  return settings.ATTIO_API_TOKEN


def _recent_attio_filter(days: int = 1) -> dict:
  now = datetime.now()
  return {ATTRIBUTE: {"active_from": {"$gt": convert_to_iso(now - timedelta(days=days))}}}


@pytest.mark.asyncio
async def test_stream_attribute_values_types_and_shapes():
  token = _require_token()

  async with AttioClient(attio_token=token) as attio_client:
    fetcher = AttioAttributeFetcher(
      attio_client=attio_client,
      concurrency=5,
      parent_object=PARENT_OBJECT,
      attribute=ATTRIBUTE,
      record_model=AttioApplicationRecord,
      attribute_model=AttioApplicationWorkflowStatusAttribute,
      filter=_recent_attio_filter(days=1),
      limit=10,
    )

    seen = 0
    async for result in fetcher.stream_attribute_values():
      assert isinstance(result.record_id, str) and result.record_id
      assert isinstance(result.values, list)
      for v in result.values:
        # Model type
        assert isinstance(v, AttioApplicationWorkflowStatusAttribute)

        # Required fields and types
        assert isinstance(v.active_from, datetime)
        assert (v.active_until is None) or isinstance(v.active_until, datetime)
        assert isinstance(v.attribute_type, str)

        # created_by_actor
        assert isinstance(v.created_by_actor.type, str)
        if v.created_by_actor.id is not None:
          assert isinstance(v.created_by_actor.id, str)

        # status
        s = v.status
        assert isinstance(s.title, str)
        assert isinstance(s.is_archived, bool)
        assert isinstance(s.celebration_enabled, bool)
        if s.target_time_in_status is not None:
          assert isinstance(s.target_time_in_status, str)

        # status.id
        sid = s.id
        assert isinstance(sid.workspace_id, str)
        assert isinstance(sid.object_id, str)
        assert isinstance(sid.attribute_id, str)
        assert isinstance(sid.status_id, str)

      seen += 1
      if seen >= 2:
        break

    if seen == 0:
      pytest.skip(
        f"No records found for object '{PARENT_OBJECT}' (attribute '{ATTRIBUTE}') to validate."
      )


@pytest.mark.asyncio
async def test_attribute_values_model_dump_is_json_serializable():
  token = _require_token()

  async with AttioClient(attio_token=token) as attio_client:
    fetcher = AttioAttributeFetcher(
      attio_client=attio_client,
      concurrency=5,
      parent_object=PARENT_OBJECT,
      attribute=ATTRIBUTE,
      record_model=AttioApplicationRecord,
      attribute_model=AttioApplicationWorkflowStatusAttribute,
      filter=_recent_attio_filter(days=1),
      limit=5,
    )
    count = 0
    async for result in fetcher.stream_attribute_values():
      # Ensure model_dump is JSON serializable for each value
      for v in result.values:
        dumped = v.model_dump()
        json.dumps(dumped, default=str)
      count += 1
      if count >= 2:
        break

    if count == 0:
      pytest.skip(
        f"No records found for object '{PARENT_OBJECT}' (attribute '{ATTRIBUTE}') to validate."
      )
