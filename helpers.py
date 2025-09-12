from datetime import datetime, timezone


def convert_to_iso(dt: datetime) -> str:
  if dt.tzinfo is None:
    dt = dt.replace(tzinfo=timezone.utc)
  return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def get_last_sync(state: dict) -> datetime | None:
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
