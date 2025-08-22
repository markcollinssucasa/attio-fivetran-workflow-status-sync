## Attio Notes Summarizer (Gemini/Vertex AI)

Fetch notes for an Attio record and summarize them with Google's Gemini models. Interactive CLI built with Typer, Questionary, Pydantic, and Rich. Uses the unified Google Gen AI SDK and can run against either the Gemini Developer API or Vertex AI based on environment variables.

Reference: Google’s unified Gen AI SDK overview: [SDKs overview](https://cloud.google.com/vertex-ai/generative-ai/docs/sdks/overview)

### Requirements
- Python 3.12+
- uv (recommended for running without local installs)
- Attio API token
- Google Gen AI credentials (either Developer API key or Vertex AI project setup)

### Install/Run
You can run directly with uvx from this project path (no local install needed):

```bash
# Set environment variables (see below), then:
uvx --from /Users/markcollins/sucasa-finance/attio-note-ai attio-notes-summarize chat \
  --parent-object applications \
  --model gemini-2.0-flash
```

Alternatively, if you prefer running the module directly:

```bash
uv run main.py  # will launch the same interactive flow
```

### Environment Configuration

- Attio
  - `ATTIO_API_TOKEN`: your Attio API token (loaded automatically from a `.env` file if present)

- Google Gen AI (choose one backend):
  - Developer API (Gemini):
    - `GOOGLE_GENERATIVE_AI_API_KEY` or `GOOGLE_API_KEY` (also auto-loaded from `.env`)
  - Vertex AI:
    - `GOOGLE_GENAI_USE_VERTEXAI=true`
    - `GOOGLE_CLOUD_PROJECT=<your-gcp-project>`
    - `GOOGLE_CLOUD_LOCATION=global` (optional; defaults to `global`)

Example (Developer API):
```bash
export ATTIO_API_TOKEN="your_attio_token"
export GOOGLE_GENERATIVE_AI_API_KEY="your_gemini_key"

uvx --from /Users/markcollins/sucasa-finance/attio-note-ai attio-notes-summarize chat \
  --parent-object people \
  --model gemini-2.0-flash
```

Using a `.env` file (auto-loaded):
```bash
cat > .env << 'EOF'
ATTIO_API_TOKEN=your_attio_token
GOOGLE_GENERATIVE_AI_API_KEY=your_gemini_key
# Or use Vertex AI instead:
# GOOGLE_GENAI_USE_VERTEXAI=true
# GOOGLE_CLOUD_PROJECT=your-gcp-project
# GOOGLE_CLOUD_LOCATION=global
EOF

uvx --from /Users/markcollins/sucasa-finance/attio-note-ai attio-notes-summarize chat
```

Example (Vertex AI):
```bash
export ATTIO_API_TOKEN="your_attio_token"
export GOOGLE_GENAI_USE_VERTEXAI=true
export GOOGLE_CLOUD_PROJECT="your-gcp-project"
export GOOGLE_CLOUD_LOCATION="global"

uvx --from /Users/markcollins/sucasa-finance/attio-note-ai attio-notes-summarize chat \
  --parent-object applications \
  --model gemini-2.0-flash
```

### Interactive Flow
When you run the `chat` command:
1. `.env` is auto-loaded; if `ATTIO_API_TOKEN` (or Gemini key) is still missing, you’ll be prompted.
2. You’ll be asked for the application `record_id` (Attio `parent_record_id`, UUID) if not passed via `--record-id`.
3. System prompt:
   - Default is built-in, or pass your own via `--system-prompt "..."`.
4. The tool fetches the application record and all notes, builds a corpus, and caches both to disk.
5. It shows an initial summary using Gemini/Vertex.
6. Interactive loop for follow‑ups. Type `exit` to quit, or `switch:<new-record-id>` to change applications mid-session (system prompt is preserved; notes/app data are reloaded).

### CLI Options
- `--parent-object` (default: `applications`)
- `--model` (default: `gemini-2.0-flash`)
- `--system-prompt` / `-s` (override the built-in system instructions)
- `--limit` (default: 50; max 50 per Attio API)
- `--max-pages` (default: 20)

### Notes Caching
- Raw application JSON: `notes_cache/notes-<parent_object>-<record_id>.application.json`
- Application context (markdown/plaintext): `notes_cache/notes-<parent_object>-<record_id>.application.md`
- Raw notes: `notes_cache/notes-<parent_object>-<record_id>.json`
- Combined notes corpus: `notes_cache/notes-<parent_object>-<record_id>.md`

### Troubleshooting
- Missing imports when opening the code are expected locally; uvx installs dependencies for the run.
- Attio 401/403: check `ATTIO_API_TOKEN`.
- Vertex AI errors: verify `GOOGLE_CLOUD_PROJECT`, `GOOGLE_CLOUD_LOCATION`, and that `GOOGLE_GENAI_USE_VERTEXAI` is set to a truthy value.
- Developer API errors: verify `GOOGLE_GENERATIVE_AI_API_KEY` or `GOOGLE_API_KEY`.

### Security
- Prefer environment variables for secrets; avoid committing tokens.
- Rotate tokens/keys periodically.