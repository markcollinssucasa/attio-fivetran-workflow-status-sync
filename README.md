## Attio ↔ Fivetran Workflow Status Sync

Small Python 3.12 project to sync Attio Application `workflow_status` history into a Fivetran destination table using the Fivetran Connector SDK. It fetches attribute values asynchronously with backpressure and upserts rows via the SDK.

### How it works
- **Fetcher**: `AttioAttributeFetcher.stream_attribute_values()` concurrently retrieves historic `workflow_status` values for Attio `applications` records.
- **Incremental**: The connector tracks `state["last_sync"]` and applies a 60‑minute buffer to avoid missed updates. If no state exists, it backfills from 30 days ago.
- **Upserts**: Each batch is written to the `attio_workflow_status` table via `op.upsert`.

### Requirements
- Python >= 3.12
- Attio API token with access to the target workspace

### Setup
1. Create a `.env` file with your Attio token:
```bash
echo "ATTIO_API_TOKEN=your_attio_token_here" > .env
```
2. Install dependencies (choose one):
```bash
# Using uv (recommended)
pip install uv
uv sync

# Or using pip
python -m venv .venv && source .venv/bin/activate
pip install -r <(python - <<'PY'
import tomllib, sys
deps = tomllib.load(open('pyproject.toml','rb'))['project']['dependencies']
print('\n'.join(deps))
PY
)
```
3. Generate requirements.txt (required for Fivetran deployments)
Fivetran's Connector SDK expects a `requirements.txt` so it can install dependencies when you `deploy`.

```bash
# If you used uv
uv export --format requirements-txt --no-hashes -o requirements.txt
# (fallback)
uv pip freeze > requirements.txt

# If you used pip
pip freeze > requirements.txt
```
Commit `requirements.txt` to version control.

### Quick smoke test (CLI)
Print a few Attio application record IDs to verify auth and connectivity:
```bash
python test_script.py
```
Notes:
- `test_script.py` uses hardcoded filter/limit for a simple sanity check.
- Ensure `ATTIO_API_TOKEN` is available in your shell or `.env`.

### Local runs and testing
You can test locally either with the Fivetran Local Tester (recommended) or by running the script directly.

Option A — Fivetran Local Tester (recommended):
```bash
pip install fivetran-connector-sdk
fivetran debug
# or include config
# fivetran debug --configuration configuration.json
```
Notes:
- Creates/updates a DuckDB file at `files/warehouse.db` with the tables written by the connector.
- Prints a detailed log; a successful run ends with "Sync SUCCEEDED".
- See the Fivetran Connector SDK Setup Guide for more details: [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/setup-guide)

Option B — Direct Python run:
```bash
python connector.py
```
What it does:
- Loads `ATTIO_API_TOKEN` from the environment.
- Computes `last_sync` from connector state (defaults to now - 30 days, with a 60‑minute safety buffer).
- Streams attribute values and upserts rows into `attio_workflow_status`.
- Persists the updated state via `op.checkpoint`.

### Configuration
- **Environment**: `ATTIO_API_TOKEN` (required)
- **Hardcoded parameters** (edit in code if needed):
  - Concurrency: `10` (in `connector.py` and `main.py`)
  - Page size `limit`: `50` (in `connector.py`) / `2` (in `main.py` demo)
  - Parent object: `applications`
  - Attribute: `workflow_status`

### Fivetran deployment (optional)
After testing locally, you can deploy this connector to Fivetran so it runs in your account.

```bash
# Ensure requirements.txt exists and is committed
fivetran deploy \
  --api-key <BASE_64_ENCODED_API_KEY> \
  --destination <DESTINATION_NAME> \
  --connection <CONNECTION_NAME>
# Optionally include configuration
# fivetran deploy --api-key <...> --destination <...> --connection <...> --configuration configuration.json
```
Then unpause/start the connection from the Fivetran UI or API. See the official guide for step‑by‑step details and examples: [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/setup-guide)

### Data model
- Declared in `schema()` within `connector.py`:
  - Table: `attio_workflow_status`
  - Primary key: `application_id`
- Each upserted row includes:
  - `application_id` (string)
  - `workflow_status` (JSON; Pydantic‑validated shape from `AttioApplicationWorkflowStatusAttribute`)

### State and time handling
- `state["last_sync"]` is stored as a Python ISO string (UTC). On each run:
  - If present and valid: parsed and used.
  - If missing/invalid: defaults to `now - 30 days`.
  - A 60‑minute buffer is applied when building the Attio filter to reduce risk of missed updates.

### Troubleshooting
- "Event loop is running" when running locally: This can occur in notebook environments because `asyncio.run(...)` is used. Run from a normal shell, or adapt with a thread runner if you must call from within an active loop.
- Attio auth errors: Verify `ATTIO_API_TOKEN` and scoping for the target workspace.

### Development
- Linting: Ruff is included; basic settings live in `pyproject.toml`.
- Typing: Python 3.12 type features are used (e.g., `list[T]`, `| None`).
- Key files:
  - `connector.py`: Fivetran connector `schema` and `update` loop
  - `attio_attribute_fetcher.py`: Async concurrent attribute fetcher
  - `attio_client.py`: Minimal Attio REST client
  - `attio_types/*`: Pydantic models for Attio records/attributes

### License
Proprietary – internal use.


