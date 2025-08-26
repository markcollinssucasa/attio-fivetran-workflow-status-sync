
from __future__ import annotations
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict


class AttioApplicationWorkflowStatusAttribute(BaseModel):
  active_from: datetime
  active_until: Optional[datetime] = None
  created_by_actor: CreatedByActor
  status: Status
  attribute_type: str

  model_config = ConfigDict(extra="ignore")


class CreatedByActor(BaseModel):
  type: str
  id: Optional[str] = None

  model_config = ConfigDict(extra="ignore")


class StatusId(BaseModel):
  workspace_id: str
  object_id: str
  attribute_id: str
  status_id: str

  model_config = ConfigDict(extra="ignore")


class Status(BaseModel):
  id: StatusId
  title: str
  is_archived: bool
  target_time_in_status: Optional[str] = None
  celebration_enabled: bool

  model_config = ConfigDict(extra="ignore")
