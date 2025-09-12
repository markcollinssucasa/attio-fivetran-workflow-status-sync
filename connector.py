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
from helpers import convert_to_iso, get_last_sync
from settings import Settings
from attio_attribute_fetcher import AttioAttributeFetcher


def schema(configuration: dict):
  return [
    {
      "table": "attio_workflow_status",
      "primary_key": ["application_id"],
      "columns": [
        {
          "name": "application_id",
          "type": "string",
        },
        {
          "name": "workflow_status",
          "type": "json",
        },
      ],
    }
  ]


async def async_update(configuration: dict, state: dict):
  settings = Settings()

  now_utc = datetime.now(timezone.utc)
  last_sync = get_last_sync(state) or now_utc - timedelta(days=30)

  last_sync_with_buffer = last_sync - timedelta(minutes=60)
  logging.info(
    f"Starting update with last sync: {last_sync} and buffer: {last_sync_with_buffer}"
  )

  attio_filter: dict[str, Any] = {}
  if last_sync is not None:
    attio_filter = {
      "workflow_status": {"active_from": {"$gt": convert_to_iso(last_sync_with_buffer)}}
    }

  try:
    async with AttioClient(attio_token=settings.ATTIO_API_TOKEN) as client:
      attio_attribute_fetcher = AttioAttributeFetcher(
        attio_client=client,
        concurrency=5,
        parent_object="applications",
        attribute="workflow_status",
        record_model=AttioApplicationRecord,
        attribute_model=AttioApplicationWorkflowStatusAttribute,
        filter=attio_filter,
      )
      async for rows in attio_attribute_fetcher.stream_attribute_values():
        try:
          op.upsert(
            table="attio_workflow_status",
            data=dict(
              application_id=rows.record_id,
              workflow_status={
                "history": [row.model_dump(mode="json") for row in rows.values]
              },
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
    asyncio.run(async_update(configuration, state))
  except Exception as err:
    logging.warning(f"Update run failed: {err}")
    raise


connector = Connector(update=update, schema=schema)


if __name__ == "__main__":
  connector.debug(configuration={})
