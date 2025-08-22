from datetime import datetime
from pydantic import BaseModel, ConfigDict


class AttioRecordId(BaseModel):
  workspace_id: str
  object_id: str
  record_id: str

  model_config = ConfigDict(extra="ignore")  # ignore unexpected fields


class AttioRecord(BaseModel):
  id: AttioRecordId
  created_at: datetime
  web_url: str

  model_config = ConfigDict(extra="ignore")  # ignore unexpected fields


class AttioApplicationRecord(AttioRecord):

  model_config = ConfigDict(extra="ignore")  # ignore unexpected fields


