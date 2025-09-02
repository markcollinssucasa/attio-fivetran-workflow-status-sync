import sys
import asyncio
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict
from attio_client import AttioClient
from attio_types.attio_application import AttioApplicationRecord
from attio_types.attio_application_workflow_status_atttribute import (
  AttioApplicationWorkflowStatusAttribute,
)
from attio_attribute_fetcher import AttioAttributeFetcher

class Settings(BaseSettings):
  ATTIO_API_TOKEN: Optional[str] = None
  model_config = SettingsConfigDict(
    env_file=".env",
    env_file_encoding="utf-8",
    extra="ignore",
  )

settings = Settings()

async def _async_main():
  attio_token = settings.ATTIO_API_TOKEN
  if not attio_token:
    print(
      "Missing ATTIO_API_TOKEN in environment. Set it in your .env file.",
      file=sys.stderr,
    )
    return 2

  async with AttioClient(attio_token=attio_token) as attio_client:
    attio_attribute_fetcher = AttioAttributeFetcher(
      attio_client=attio_client,
      concurrency=10,
      parent_object="applications",
      attribute="workflow_status",
      record_model=AttioApplicationRecord,
      attribute_model=AttioApplicationWorkflowStatusAttribute,
      filter={"workflow_status": {"active_from": {"$gt": "2025-08-22T00:00:00Z"}}},
      limit=2,
    )
    print("Starting attribute fetcher")
    async for result in attio_attribute_fetcher.stream_attribute_values():
      print(result.record_id)
      print([result.model_dump() for result in result.values])
      print("--------------------------------")


def main(argv: Optional[list[str]] = None) -> int:

  try:
    asyncio.run(_async_main())
    return 0
  except KeyboardInterrupt:
    return 130


if __name__ == "__main__":
  raise SystemExit(main())
