import os
import sys
import json
import asyncio
from typing import Optional
from pprint import pprint

from dotenv import load_dotenv
from pathlib import Path
from attio_client import AttioClient
from attio_types.attio_application import AttioApplicationRecord
from attio_types.attio_application_workflow_status_atttribute import (
  AttioApplicationWorkflowStatusAttribute,
)
from attio_attribute_fetcher import AttioAttributeFetcher


def _load_env_files() -> None:
  """Load environment variables from .env in project root and/or CWD."""
  project_env = Path(__file__).resolve().parent / ".env"
  cwd_env = Path.cwd() / ".env"
  for env_path in (project_env, cwd_env):
    try:
      if env_path.exists():
        load_dotenv(dotenv_path=str(env_path), override=False)
    except Exception:
      # Do not fail if .env loading has issues; environment may be already set
      pass


async def _async_main():
  attio_token = os.getenv("ATTIO_API_TOKEN")
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

    async for result in attio_attribute_fetcher.stream_attribute_values():
      print(result.record_id)


def main(argv: Optional[list[str]] = None) -> int:
  """Entry point for CLI usage."""
  _load_env_files()

  try:
    asyncio.run(_async_main())
    return 0
  except KeyboardInterrupt:
    return 130


if __name__ == "__main__":
  raise SystemExit(main())
