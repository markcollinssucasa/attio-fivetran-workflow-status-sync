import asyncio
from datetime import datetime, timezone, timedelta
from typing import Any

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as logging
from fivetran_connector_sdk import Operations as op

from attio_client import AttioClient
from attio_types.attio_application import AttioApplicationRecord
from attio_types.attio_application_workflow_status_atttribute import (
  AttioApplicationWorkflowStatusAttribute,
)
from helpers import _get_env, _iso
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


async def _update(configuration: dict, state: dict):
  token = _get_env("ATTIO_API_TOKEN")

  now = datetime.now(timezone.utc)
  raw_last_sync = state.get("last_sync")
  if isinstance(raw_last_sync, str) and raw_last_sync:
    try:
      last_sync = datetime.fromisoformat(raw_last_sync)
    except Exception:
      last_sync = now - timedelta(days=30)
  elif isinstance(raw_last_sync, datetime):
    last_sync = raw_last_sync
  else:
    last_sync = now - timedelta(days=30)

  last_sync_with_buffer = last_sync - timedelta(minutes=60)

  attio_filter: dict[str, Any] = {}
  if last_sync is not None:
    attio_filter = {
      "workflow_status": {"active_from": {"$gt": _iso(last_sync_with_buffer)}}
    }

  try:
    async with AttioClient(attio_token=token) as client:
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
              workflow_status=[row.model_dump() for row in rows.values],
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
    state["last_sync"] = str(now)
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
