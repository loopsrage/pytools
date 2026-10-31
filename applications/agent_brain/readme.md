# Agent Brain - Site Intelligence Pipeline

A modular, self-contained FastAPI application that periodically scrapes selected
sites, cleans the extracted content via ETL, organizes artifacts by topic (site),
and indexes everything into a local Qdrant vector database. Agent-controllable
through Model Context Protocol (MCP) endpoints and a CLI for model slotting.

## Structure

```
applications/agent_brain/
├── main.py                  # FastAPI app + argument processing (host/port/lifespan)
├── cli/                     # Model slotting CLI (multi-agent)
├── core/                    # Core brain infrastructure
│   ├── brain/               # Topics, pages, artifacts store + orchestrator
│   └── settings/            # YAML configuration accessor
├── ingest/                  # Data ingestion pipeline
│   ├── scraper/             # Playwright scraping + fuzzy dedup
│   ├── qdrant_ingest/       # PeriodicProducer → QueueControllers → Qdrant
│   ├── etl/                 # CSV/JSON cleaning + transform
│   └── doc_pipeline/        # OCR + document indexing (pycv, pytesseract, pypdfium2)
├── models/                  # Model management
│   ├── model_registry/      # Discovers models under models/store/
│   ├── model_harness/       # Thread-safe multi-agent harness (.pt / .safetensors)
│   ├── architectures/       # Registered nn.Module architectures
│   └── store/               # Model repository (.pt / .safetensors files)
├── ops/                     # Production-grade operations tooling
│   ├── audit/               # Tamper-evident, hash-chained audit trails per agent
│   ├── resilience/          # Per-agent circuit breakers, rate limiters, retries
│   ├── feature_flags/       # Percentage rollouts, user/attribute targeting
│   ├── secrets/             # Secure credential injection (env + files)
│   └── webhook/             # Reliable webhook delivery with retries + dead-letter
├── mcp/                     # MCP servers (agent_brain + fs_jail)
└── tests/                   # Test suite
```

## Quick Start

### Prerequisites

From the repo root (`toolset/`):

```bash
# Ensure Playwright Chromium is installed (once)
uv run -m playwright install chromium
```

### Run the service

```bash
cd toolset
uv run uvicorn applications.agent_brain.main:app --reload --host 0.0.0.0 --port 8127
```

The app auto-starts the Qdrant ingestion pipeline on startup (scans `data/brain/` every
60s, chunks text, TF-IDF embeds, upserts to `data/.qdrant_brain`).

### REST Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Service health + Qdrant stats |
| GET | `/settings` | All `agent_brain` settings (JSON) |
| GET | `/settings/yaml` | Settings as YAML |
| GET | `/settings/json` | Settings as JSON bytes |
| POST | `/topics?name=&url=` | Register a site (checks `robots.txt`) |
| GET | `/topics` | List registered topics |
| POST | `/topics/{id}/scrape` | Scrape a topic → ETL → CSV artifact |
| GET | `/topics/{id}/pages` | Scraped pages for a topic |
| GET | `/topics/{id}/artifacts` | ETL artifacts for a topic |
| GET | `/artifacts/{id}/content` | Artifact content (CSV/JSON) |
| POST | `/qdrant/start` | Start periodic ingestion pipeline |
| POST | `/qdrant/stop` | Stop ingestion pipeline |
| GET | `/qdrant/stats` | Ingestion statistics |

### MCP (agent control)

Two JSON-RPC 2.0 endpoints:

| Endpoint | Purpose |
|----------|---------|
| `POST /api/mcp` | Agent brain: topics, scrape, Qdrant search/control, model slotting |
| `POST /api/mcp/fs_jail` | Isolated per-GUID filesystem jails |

Example (`curl`):

```bash
# Register a topic via MCP
curl -s -X POST http://localhost:8127/api/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"agent_brain_register_topic","arguments":{"name":"example","url":"https://example.com"}}}'

# List available models via MCP
curl -s -X POST http://localhost:8127/api/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"agent_model_list","arguments":{}}}'

# Create an isolated filesystem jail via MCP
curl -s -X POST http://localhost:8127/api/mcp/fs_jail \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"fs_jail_create","arguments":{"guid":"my-jail"}}}'

# Write into the jail (path traversal is blocked)
curl -s -X POST http://localhost:8127/api/mcp/fs_jail \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"fs_jail_write","arguments":{"guid":"my-jail","path":"notes.txt","content":"hello"}}}'
```

MCP info (tools/resources/prompts): `GET /api/mcp` and `GET /api/mcp/fs_jail`.

### Dual Hash Layer (scrape deduplication)

The agent brain uses a two-layer hash system to prevent redundant scraping and
vector storage:

1. **URL hash** (`url_hash`): Deterministic SHA-256 of the page URL. Used as the
   Qdrant point ID base, so re-scraping the same URL upserts (replaces) rather
   than creating duplicate points.

2. **Content hash** (`content_hash`): SHA-256 (truncated to 16 chars) of the page
   text content. Stored in the Qdrant payload. Before upserting new chunks, the
   pipeline checks if an existing point for the same URL has the same content hash
   — if so, the upsert is skipped entirely.

3. **24-hour scrape cooldown**: A page is not scraped more than once per day.
   Before scraping, the orchestrator checks if a page with the same URL hash was
   scraped within the last 24 hours — if so, the scrape is skipped.

Example scrape result showing the dual hash layer:
```json
{
  "topic": "example",
  "page_id": "a6cdcd70-...",
  "url_hash": "3641c5f2...",
  "content_hash": "290f493c...",
  "skipped": false,
  "reason": null
}
```

If the content hasn't changed since the last scrape:
```json
{
  "topic": "example",
  "skipped": true,
  "reason": "content unchanged",
  "url_hash": "3641c5f2...",
  "content_hash": "290f493c..."
}
```

If the page was scraped within the last 24 hours:
```json
{
  "topic": "example",
  "skipped": true,
  "reason": "scraped within last 24 hours",
  "url_hash": "3641c5f2..."
}
```

### Operations Tooling (ops/)

Production-grade concerns for running agents at scale. Each module wraps a
tool from `src/` and scopes it to the agent brain:

| Module | Purpose |
|--------|---------|
| `ops/audit` | Tamper-evident, hash-chained audit trails per agent |
| `ops/resilience` | Per-agent circuit breakers, rate limiters, retries |
| `ops/feature_flags` | Percentage rollouts, user/attribute targeting |
| `ops/secrets` | Secure credential injection (env + files) |
| `ops/webhook` | Reliable webhook delivery with retries + dead-letter |

**MCP tools** (on `POST /api/mcp`):

| Tool | Description |
|------|-------------|
| `agent_audit_append` | Append an audit entry for an agent |
| `agent_audit_verify` | Verify hash-chain integrity for an agent |
| `agent_audit_entries` | List all audit entries for an agent |
| `agent_resilience_state` | Return an agent's circuit breaker state |
| `agent_feature_flag` | Check if a feature flag is enabled |
| `agent_secret_get` | Resolve a secret by name |
| `agent_webhook_enqueue` | Enqueue a webhook delivery |
| `agent_webhook_process` | Process pending webhook deliveries |

**MCP resources:**

| URI | Description |
|-----|-------------|
| `agent_brain://ops/audit` | All audit entries grouped by agent |
| `agent_brain://ops/resilience` | Per-agent circuit breaker states |
| `agent_brain://ops/feature_flags` | All feature flag definitions |

### Model Slotting CLI (thread-safe, multi-agent)

The model harness is thread-safe: each agent is identified by an `agent_id` and gets
its own isolated active-model slot. Loading the same model for the same agent is
deduplicated via `Onceler`, so concurrent callers share the result. Models live under
`applications/agent_brain/models/` — one subfolder per model containing `config.json`
+ `model.pt` or `model.safetensors`. Drop a new folder to add a model; no code changes
needed.

```bash
# List available models
python applications/agent_brain/cli/__init__.py list

# Load + run a model for a specific agent in one shot
python applications/agent_brain/cli/__init__.py run --agent-id agent-a sentiment-classifier forward --input 0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5

# Run with the default agent ('default')
python applications/agent_brain/cli/__init__.py run sentiment-classifier forward --input 0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5

# Load, inspect, run, unload for a specific agent
python applications/agent_brain/cli/__init__.py load --agent-id agent-a text-embedder
python applications/agent_brain/cli/__init__.py active --agent-id agent-a
python applications/agent_brain/cli/__init__.py run --agent-id agent-a sentiment-classifier info
python applications/agent_brain/cli/__init__.py unload --agent-id agent-a

# Interactive REPL session (persistent loaded model per agent)
python applications/agent_brain/cli/__init__.py interactive
#   agent-brain[default]> load sentiment-classifier
#   agent-brain[default]> agent agent-b
#   agent-brain[agent-b]> load text-embedder
#   agent-brain[agent-b]> run forward --input 0.3,0.3,0.3,0.3,0.3,0.3,0.3,0.3
```

MCP model tools (on `POST /api/mcp`) also accept an `agent_id` parameter (defaults to
`"default"`):

| Tool | Description |
|------|-------------|
| `agent_model_list` | List all available models |
| `agent_model_load` | Load a model for an agent (`agent_id`, `model_id`) |
| `agent_model_unload` | Unload an agent's active model (`agent_id`) |
| `agent_model_active` | Return an agent's active model (`agent_id`) |
| `agent_model_run` | Run an action with an agent's active model (`agent_id`, `action`, `input_data`) |

### Configuration

Edit `applications/agent_brain/settings/settings.yaml`. Keys are loaded via the
repo's `settings` module and exposed as dotted paths (e.g. `scraper.headless`,
`api.port`, `qdrant.scan_interval_seconds`, `fs_jail.base_dir`).

A shared Gmail account can be configured under `agent_brain.email`:

```yaml
  email:
    enabled: true
    provider: "gmail"
    smtp_host: "smtp.gmail.com"
    smtp_port: 587
    use_tls: true
    username: "shared.account@gmail.com"
    from_name: "Agent Brain"
```

The Gmail app password is **not** stored in settings.yaml. It is resolved from a
secret via `AgentSecrets`: environment variable `GMAIL_APP_PASSWORD` or the file
pointed to by `GMAIL_APP_PASSWORD_FILE`.

### Email (MCP)

Agent-controllable email sending + Qdrant-backed template storage (alerts and
outbound job application emails):

| Tool | Description |
|------|-------------|
| `agent_email_send` | Send an email via the configured Gmail account, directly or by rendering a stored template. |
| `agent_email_send_sms` | Send an SMS to a phone number via the Google Fi email-to-SMS gateway (`digits@msg.fi.google.com`). |
| `agent_email_template_write` | Write/update an email template in Qdrant (bodies use `{variable}` placeholders). |
| `agent_email_template_list` | List all stored email templates. |
| `agent_email_template_get` | Get a stored email template by id. |
| `agent_email_template_delete` | Delete a stored email template by id. |
| `agent_email_template_search` | Semantically search stored email templates. |
| `agent_email_template_render` | Render a stored template's subject/body with variables (no send). |

Resource: `agent_brain://email/templates` (all stored templates, JSON).

Templates live in a dedicated local Qdrant collection (`email_templates`) under
`data/.qdrant_email_templates` and are indexed with TF-IDF vectors for semantic
search, so an agent can retrieve the right alert or outbound template by query.

### SMS via Google Fi

`GmailSender.send_sms(to, body, ...)` routes to Google Fi's email-to-SMS
gateway: a digit-only `to` (e.g. `8017091149`) is converted to
`8017091149@msg.fi.google.com`. There is also `send_templated_sms` for rendering
a `{variable}` template body before sending. The MCP tool
`agent_email_send_sms` exposes this, accepting either a raw phone number or a
full `@msg.fi.google.com` address.


## Legal / Compliance

- Every registered topic is checked against the site's `robots.txt` before scraping.
- Blocked sites are marked `BLOCKED` and skipped.
- `crawl-delay` from `robots.txt` is respected.
- Only sites you explicitly register are ever scraped.

## Tests

Run the full suite from the repo root:

```bash
uv run pytest src/ applications/ --ignore=src/indexes/informational_index
```

Tests live in `applications/agent_brain/tests/`.
