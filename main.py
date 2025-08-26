import os
import sys
import json
import asyncio
from typing import Optional
from pprint import pprint

from dotenv import load_dotenv
from pathlib import Path
from attio_client import AttioClient
from types.attio_application import AttioApplicationRecord
from types.attio_application_workflow_status_atttribute import AttioApplicationWorkflowStatusAttribute

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
    values = await attio_client.query_records(
      model=AttioApplicationRecord,
      parent_object="applications",
      filter={"workflow_status": {"active_from": {"$gt": "2025-08-22T00:00:00Z"}}},
      limit=1,
      offset=3,
    )

    list_attribute_values = await attio_client.list_attribute_values(
      model=AttioApplicationWorkflowStatusAttribute,
      parent_object="applications",
      record_id=values[0].id.record_id,
      attribute="workflow_status",
      show_historic=True,
    )
    serialized = [item.model_dump(mode="json") for item in list_attribute_values]
    pprint(json.dumps(serialized, ensure_ascii=False))


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
