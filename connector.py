import asyncio
from datetime import datetime, timezone, timedelta
from re import S
from typing import Any

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as logging
from fivetran_connector_sdk import Operations as op

from attio_client import AttioClient
from attio_types.attio_application import AttioApplicationRecord
from attio_types.attio_application_workflow_status_atttribute import (
  AttioApplicationWorkflowStatusAttribute,
)
from helpers import _iso
from settings import Settings
from attio_attribute_fetcher import AttioAttributeFetcher


def validate_configuration(configuration: dict):
  if "attio_api_token" not in configuration:
    raise ValueError("Could not find 'attio_api_token'")


def schema(configuration: dict):
  return [
    {
      "table": "attio_workflow_status",
      "primary_key": ["application_id"],
    }
  ]


def _get_last_sync(state: dict) -> datetime|None:

  raw_value = state.get("last_sync") if isinstance(state, dict) else None

  if isinstance(raw_value, datetime):
    return raw_value if raw_value.tzinfo else raw_value.replace(tzinfo=timezone.utc)

  if isinstance(raw_value, str) and raw_value.strip():
    text_value = raw_value.strip()
    try:
      try:
        parsed = datetime.fromisoformat(text_value)
      except ValueError:
        parsed = datetime.fromisoformat(text_value.replace("Z", "+00:00"))
      return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
      return None

  return None


async def _update(configuration: dict, state: dict):
  settings = Settings()

  now_utc = datetime.now(timezone.utc)
  last_sync = _get_last_sync(state) or now_utc - timedelta(days=2)
  
  last_sync_with_buffer = last_sync - timedelta(minutes=60)
  logging.info(f"Starting update with last sync: {last_sync} and buffer: {last_sync_with_buffer}")

  attio_filter: dict[str, Any] = {}
  if last_sync is not None:
    attio_filter = {
      "workflow_status": {"active_from": {"$gt": _iso(last_sync_with_buffer)}}
    }

  try:
    async with AttioClient(attio_token=settings.ATTIO_API_TOKEN) as client:
      attio_attribute_fetcher = AttioAttributeFetcher(
        attio_client=client,
        concurrency=10,
        parent_object="applications",
        attribute="workflow_status",
        record_model=AttioApplicationRecord,
        attribute_model=AttioApplicationWorkflowStatusAttribute,
        filter=attio_filter,
        limit=50,
      )
      async for rows in attio_attribute_fetcher.stream_attribute_values():
        try:
          op.upsert(
            table="attio_workflow_status",
            data=dict(
              application_id=rows.record_id,
              workflow_status={"history":[row.model_dump(mode="json") for row in rows.values]},
            ),
          )
        except Exception as err:
          logging.warning(f"Upsert failed for application_id={rows.record_id}: {err}")
          continue
        logging.info(f"Data fetch loop successful for application_id={rows.record_id}")
  except Exception as err:
    logging.warning(f"Data fetch loop failed: {err}")
    raise

  try:
    state["last_sync"] = str(now_utc)
    op.checkpoint(state=state)
  except Exception as err:
    logging.warning(f"Checkpoint failed: {err}")
    raise


def update(configuration: dict, state: dict):
  try:
    asyncio.run(_update(configuration, state))
  except Exception as err:
    logging.warning(f"Update run failed: {err}")
    raise


connector = Connector(update=update, schema=schema)


if __name__ == "__main__":
  connector.debug(configuration={})
