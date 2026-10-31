"""
Model Context Protocol (MCP) Server for the Agent Brain Site Intelligence Pipeline.

Provides MCP tools and resources so an agent can:
- Register and scrape sites (topics)
- List topics, pages, and artifacts
- Retrieve artifact content
- Search Qdrant for ingested chunks
- Control the Qdrant ingestion pipeline (start/stop/stats)

Supports stdio JSON-RPC 2.0 and FastAPI streamable HTTP mounting.
"""

from __future__ import annotations

import asyncio
import json
import sys
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional

_REPO_SRC = str(Path(__file__).resolve().parents[4] / "src")
if _REPO_SRC not in sys.path:
    sys.path.insert(0, _REPO_SRC)

from applications.agent_brain.ingest.qdrant_ingest.ingest import (
    QdrantIngestPipeline,
)

MCP_SERVER_NAME = "agent-brain-intelligence"
MCP_SERVER_VERSION = "1.0.0"
DEFAULT_MCP_PROTOCOL_VERSION = "2024-11-05"


class AgentBrainMCPServer:
    """MCP server exposing the Agent Brain pipeline as tools/resources."""

    def __init__(
        self,
        orchestrator=None,
        qdrant_pipeline: Optional[QdrantIngestPipeline] = None,
        brain_store=None,
        model_harness=None,
        audit_log=None,
        resilience=None,
        feature_flags=None,
        secrets=None,
        webhook_queue=None,
        email_sender=None,
        template_store=None,
        lazy_init: bool = True,
    ):
        self._orchestrator = orchestrator
        self._qdrant = qdrant_pipeline
        self._brain_store = brain_store
        self._harness = model_harness
        self._audit = audit_log
        self._resilience = resilience
        self._feature_flags = feature_flags
        self._secrets = secrets
        self._webhook = webhook_queue
        self._email = email_sender
        self._templates = template_store
        self._initialized = False

    # ------------------------------------------------------------------
    # MCP manifest
    # ------------------------------------------------------------------
    @staticmethod
    def get_tools_manifest() -> List[Dict[str, Any]]:
        return [
            {
                "name": "agent_brain_register_topic",
                "description": (
                    "Register a new site (topic) to be scraped. Checks robots.txt for legality; "
                    "blocked sites are marked as BLOCKED and skipped."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "Human-readable name for the topic/site.",
                        },
                        "url": {
                            "type": "string",
                            "description": "URL of the site to scrape.",
                        },
                        "scrape_interval_seconds": {
                            "type": "integer",
                            "description": "Seconds between automatic scrapes (default: 3600).",
                            "default": 3600,
                        },
                        "crawl": {
                            "type": "boolean",
                            "description": "If true, crawl the site discovering job links and scrape each page (default: true).",
                            "default": True,
                        },
                        "max_pages": {
                            "type": "integer",
                            "description": "Maximum pages to crawl per topic (default: 20).",
                            "default": 20,
                        },
                        "ignore_robots": {
                            "type": "boolean",
                            "description": "Ignore robots.txt for this topic (default: false).",
                            "default": False,
                        },
                    },
                    "required": ["name", "url"],
                },
            },
            {
                "name": "agent_brain_crawl_topic",
                "description": (
                    "Crawl a registered topic: discover job links from the seed page, "
                    "scrape each job detail page, and produce ETL artifacts for each."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "topic_id": {
                            "type": "string",
                            "description": "ID of the topic to crawl.",
                        }
                    },
                    "required": ["topic_id"],
                },
            },
            {
                "name": "agent_brain_scrape_topic",
                "description": (
                    "Immediately scrape a registered topic: fetch the page, clean the text, "
                    "store it as a page, and produce a CSV artifact."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "topic_id": {
                            "type": "string",
                            "description": "ID of the topic to scrape.",
                        }
                    },
                    "required": ["topic_id"],
                },
            },
            {
                "name": "agent_brain_list_topics",
                "description": "List all registered topics (sites) and their status.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                },
            },
            {
                "name": "agent_brain_list_pages",
                "description": "List all scraped pages for a given topic.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "topic_id": {
                            "type": "string",
                            "description": "ID of the topic.",
                        }
                    },
                    "required": ["topic_id"],
                },
            },
            {
                "name": "agent_brain_list_artifacts",
                "description": "List all ETL artifacts for a given topic.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "topic_id": {
                            "type": "string",
                            "description": "ID of the topic.",
                        }
                    },
                    "required": ["topic_id"],
                },
            },
            {
                "name": "agent_brain_get_artifact",
                "description": "Retrieve the content of a stored artifact (CSV/JSON).",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "artifact_id": {
                            "type": "string",
                            "description": "ID of the artifact.",
                        }
                    },
                    "required": ["artifact_id"],
                },
            },
            {
                "name": "agent_brain_qdrant_search",
                "description": (
                    "Search Qdrant for ingested content chunks matching a query. "
                    "Supports optional filters on structured job fields: salary_min_usd, "
                    "location, remote, drug_testing, portfolio_required."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Search query text.",
                        },
                        "top_k": {
                            "type": "integer",
                            "description": "Maximum number of results (default: 5).",
                            "default": 5,
                        },
                        "salary_min_usd": {
                            "type": "integer",
                            "description": "Minimum annual salary in USD.",
                        },
                        "location": {
                            "type": "string",
                            "description": "Location filter (city or state name).",
                        },
                        "remote": {
                            "type": "boolean",
                            "description": "Filter for remote jobs (true) or non-remote (false).",
                        },
                        "drug_testing": {
                            "type": "boolean",
                            "description": "Filter for jobs requiring drug testing (true) or not (false).",
                        },
                        "portfolio_required": {
                            "type": "boolean",
                            "description": "Filter for jobs requiring a portfolio (true) or not (false).",
                        },
                    },
                    "required": ["query"],
                },
            },
            {
                "name": "agent_brain_qdrant_search_rrf",
                "description": (
                    "Run multiple Qdrant searches and fuse results with Reciprocal Rank Fusion (RRF). "
                    "Supports optional filters on structured job fields: salary_min_usd, location, "
                    "remote, drug_testing, portfolio_required."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "queries": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "List of search queries to fuse.",
                        },
                        "top_k": {
                            "type": "integer",
                            "description": "Maximum number of fused results (default: 10).",
                            "default": 10,
                        },
                        "k": {
                            "type": "integer",
                            "description": "RRF constant (default: 60).",
                            "default": 60,
                        },
                        "salary_min_usd": {
                            "type": "integer",
                            "description": "Minimum annual salary in USD.",
                        },
                        "location": {
                            "type": "string",
                            "description": "Location filter (city or state name).",
                        },
                        "remote": {
                            "type": "boolean",
                            "description": "Filter for remote jobs (true) or non-remote (false).",
                        },
                        "drug_testing": {
                            "type": "boolean",
                            "description": "Filter for jobs requiring drug testing (true) or not (false).",
                        },
                        "portfolio_required": {
                            "type": "boolean",
                            "description": "Filter for jobs requiring a portfolio (true) or not (false).",
                        },
                    },
                    "required": ["queries"],
                },
            },
            {
                "name": "agent_brain_qdrant_stats",
                "description": "Return Qdrant ingestion pipeline statistics.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                },
            },
            {
                "name": "agent_brain_search_results",
                "description": (
                    "List periodic search results. Each result is a Qdrant match "
                    "from a scheduled search term, flagged as new/seen."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "term": {
                            "type": "string",
                            "description": "Filter by search term (optional).",
                        },
                        "unseen_only": {
                            "type": "boolean",
                            "description": "Only return new/unseen results (default: false).",
                            "default": False,
                        },
                    },
                },
            },
            {
                "name": "agent_brain_mark_seen",
                "description": "Mark a search result as seen (clears the new flag).",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "search_id": {
                            "type": "string",
                            "description": "ID of the search result to mark as seen.",
                        }
                    },
                    "required": ["search_id"],
                },
            },
            {
                "name": "agent_brain_mark_all_seen",
                "description": "Mark all search results as seen.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                },
            },
            {
                "name": "agent_brain_run_search",
                "description": (
                    "Manually trigger the periodic search: crawl all topics, "
                    "index into Qdrant, then run all configured search terms."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                },
            },
            {
                "name": "agent_brain_qdrant_start",
                "description": "Start the periodic Qdrant ingestion pipeline (FS -> chunks -> Qdrant).",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                },
            },
            {
                "name": "agent_brain_qdrant_stop",
                "description": "Stop the periodic Qdrant ingestion pipeline.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                },
            },
            {
                "name": "agent_brain_health",
                "description": "Return overall health of the Agent Brain service.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                },
            },
            {
                "name": "agent_model_list",
                "description": "List all available models in the models folder.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                },
            },
            {
                "name": "agent_model_load",
                "description": "Load a model and make it the active (hot-swapped) model for the agent.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "agent_id": {
                            "type": "string",
                            "description": "Agent identifier (isolated model slot). Defaults to 'default'.",
                            "default": "default",
                        },
                        "model_id": {
                            "type": "string",
                            "description": "ID of the model to load (folder name under models/).",
                        }
                    },
                    "required": ["model_id"],
                },
            },
            {
                "name": "agent_model_unload",
                "description": "Unload the agent's currently active model.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "agent_id": {
                            "type": "string",
                            "description": "Agent identifier (isolated model slot). Defaults to 'default'.",
                            "default": "default",
                        }
                    },
                },
            },
            {
                "name": "agent_model_active",
                "description": "Return the agent's currently active (loaded) model, if any.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "agent_id": {
                            "type": "string",
                            "description": "Agent identifier (isolated model slot). Defaults to 'default'.",
                            "default": "default",
                        }
                    },
                },
            },
            {
                "name": "agent_model_run",
                "description": "Run an action with the agent's active model (info, tensors, forward/predict).",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "agent_id": {
                            "type": "string",
                            "description": "Agent identifier (isolated model slot). Defaults to 'default'.",
                            "default": "default",
                        },
                        "action": {
                            "type": "string",
                            "description": "Action to run: info, tensors, forward, or predict.",
                            "default": "info",
                        },
                        "input_data": {
                            "type": "array",
                            "items": {"type": "number"},
                            "description": "Input values for forward/predict (list of floats).",
                        },
                    },
                    "required": ["action"],
                },
            },
            {
                "name": "agent_audit_append",
                "description": "Append an audit entry for an agent (tamper-evident, hash-chained).",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "agent_id": {
                            "type": "string",
                            "description": "Agent identifier.",
                            "default": "default",
                        },
                        "actor": {
                            "type": "string",
                            "description": "Who performed the action.",
                        },
                        "action": {
                            "type": "string",
                            "description": "What action was performed.",
                        },
                        "resource": {
                            "type": "string",
                            "description": "What resource was acted upon.",
                        },
                        "details": {
                            "type": "object",
                            "description": "Additional details about the action.",
                            "default": {},
                        },
                    },
                    "required": ["actor", "action", "resource"],
                },
            },
            {
                "name": "agent_audit_verify",
                "description": "Verify hash-chain integrity for an agent's audit trail.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "agent_id": {
                            "type": "string",
                            "description": "Agent identifier.",
                            "default": "default",
                        }
                    },
                },
            },
            {
                "name": "agent_audit_entries",
                "description": "List all audit entries for an agent.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "agent_id": {
                            "type": "string",
                            "description": "Agent identifier.",
                            "default": "default",
                        }
                    },
                },
            },
            {
                "name": "agent_resilience_state",
                "description": "Return an agent's circuit breaker state.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "agent_id": {
                            "type": "string",
                            "description": "Agent identifier.",
                            "default": "default",
                        }
                    },
                },
            },
            {
                "name": "agent_feature_flag",
                "description": "Check if a feature flag is enabled for an agent/user.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "Feature flag name.",
                        },
                        "user_id": {
                            "type": "string",
                            "description": "Optional user ID for targeting.",
                        },
                    },
                    "required": ["name"],
                },
            },
            {
                "name": "agent_secret_get",
                "description": "Resolve a secret by name (env var or file).",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "Secret name.",
                        }
                    },
                    "required": ["name"],
                },
            },
            {
                "name": "agent_webhook_enqueue",
                "description": "Enqueue a webhook delivery for reliable async sending.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "url": {
                            "type": "string",
                            "description": "Webhook URL.",
                        },
                        "event": {
                            "type": "string",
                            "description": "Event name.",
                        },
                        "payload": {
                            "type": "object",
                            "description": "JSON payload to send.",
                            "default": {},
                        },
                        "max_attempts": {
                            "type": "integer",
                            "description": "Max retry attempts.",
                            "default": 3,
                        }
                    },
                    "required": ["url", "event"],
                },
            },
            {
                "name": "agent_webhook_process",
                "description": "Process all pending webhook deliveries.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                },
            },
            {
                "name": "agent_email_send",
                "description": "Send an email through the configured shared Gmail account. "
                "Either provide subject/body directly, or a template_name with template_vars "
                "to render a stored Qdrant template first.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "to": {
                            "type": "string",
                            "description": "Recipient email address.",
                        },
                        "subject": {
                            "type": "string",
                            "description": "Email subject (ignored when template_name is given).",
                        },
                        "body": {
                            "type": "string",
                            "description": "Email body (ignored when template_name is given).",
                        },
                        "template_name": {
                            "type": "string",
                            "description": "Name of a stored Qdrant template to render and send.",
                        },
                        "template_vars": {
                            "type": "object",
                            "description": "Variables to render into the template.",
                            "default": {},
                        },
                        "from_name": {
                            "type": "string",
                            "description": "Override the sender display name.",
                        },
                    },
                    "required": ["to"],
                },
            },
            {
                "name": "agent_email_send_sms",
                "description": "Send an SMS to a phone number via the Google Fi email-to-SMS "
                "gateway (number@msg.fi.google.com).",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "to": {
                            "type": "string",
                            "description": "Phone number (digits only) or full @msg.fi.google.com address.",
                        },
                        "body": {
                            "type": "string",
                            "description": "SMS message body.",
                        },
                        "subject": {
                            "type": "string",
                            "description": "Email subject line (default: 'Agent Brain').",
                        },
                        "template_body": {
                            "type": "string",
                            "description": "Template body with {variable} placeholders (rendered before send).",
                        },
                        "template_vars": {
                            "type": "object",
                            "description": "Variables to render into template_body.",
                            "default": {},
                        },
                        "from_name": {
                            "type": "string",
                            "description": "Override the sender display name.",
                        },
                    },
                    "required": ["to"],
                },
            },
            {
                "name": "agent_email_template_write",
                "description": "Write or update an email template in Qdrant (alerts or outbound job "
                "application emails). Bodies use {variable} placeholders.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "template_id": {
                            "type": "string",
                            "description": "Stable identifier for the template.",
                        },
                        "name": {
                            "type": "string",
                            "description": "Human-readable template name.",
                        },
                        "category": {
                            "type": "string",
                            "description": "Template category (e.g. alert, job_application, outbound).",
                        },
                        "subject": {
                            "type": "string",
                            "description": "Email subject with optional {variable} placeholders.",
                        },
                        "body": {
                            "type": "string",
                            "description": "Email body with optional {variable} placeholders.",
                        },
                        "variables": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "List of variable names used in the template.",
                            "default": [],
                        },
                    },
                    "required": ["template_id", "name", "category", "subject", "body"],
                },
            },
            {
                "name": "agent_email_template_list",
                "description": "List all stored email templates.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                },
            },
            {
                "name": "agent_email_template_get",
                "description": "Get a stored email template by template_id.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "template_id": {
                            "type": "string",
                            "description": "ID of the template to retrieve.",
                        }
                    },
                    "required": ["template_id"],
                },
            },
            {
                "name": "agent_email_template_delete",
                "description": "Delete a stored email template by template_id.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "template_id": {
                            "type": "string",
                            "description": "ID of the template to delete.",
                        }
                    },
                    "required": ["template_id"],
                },
            },
            {
                "name": "agent_email_template_search",
                "description": "Semantically search stored email templates by query text.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Search query text.",
                        },
                        "top_k": {
                            "type": "integer",
                            "description": "Maximum number of results (default: 5).",
                            "default": 5,
                        },
                    },
                    "required": ["query"],
                },
            },
            {
                "name": "agent_email_template_render",
                "description": "Render a stored template's subject and body with variables "
                "(does not send).",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "template_id": {
                            "type": "string",
                            "description": "ID of the template to render.",
                        },
                        "template_vars": {
                            "type": "object",
                            "description": "Variables to render into the template.",
                            "default": {},
                        },
                    },
                    "required": ["template_id"],
                },
            },
        ]

    @staticmethod
    def get_resources_manifest() -> List[Dict[str, Any]]:
        return [
            {
                "uri": "agent_brain://topics",
                "name": "All Topics",
                "description": "List of all registered topics (sites).",
                "mimeType": "application/json",
            },
            {
                "uri": "agent_brain://settings",
                "name": "Agent Brain Settings",
                "description": "All agent_brain configuration from settings.yaml.",
                "mimeType": "application/json",
            },
            {
                "uri": "agent_brain://ops/audit",
                "name": "Audit Trail",
                "description": "Agent audit log entries.",
                "mimeType": "application/json",
            },
            {
                "uri": "agent_brain://ops/resilience",
                "name": "Resilience State",
                "description": "Per-agent circuit breaker states.",
                "mimeType": "application/json",
            },
            {
                "uri": "agent_brain://ops/feature_flags",
                "name": "Feature Flags",
                "description": "All feature flag definitions.",
                "mimeType": "application/json",
            },
            {
                "uri": "agent_brain://email/templates",
                "name": "Email Templates",
                "description": "All stored email templates (Qdrant).",
                "mimeType": "application/json",
            },
        ]

    @staticmethod
    def get_prompts_manifest() -> List[Dict[str, Any]]:
        return [
            {
                "name": "trace_topic_pipeline",
                "description": "Trace the full scrape -> ETL -> Qdrant pipeline for a topic.",
                "arguments": [
                    {
                        "name": "topic_id",
                        "description": "ID of the topic to trace.",
                        "required": True,
                    }
                ],
            },
        ]

    # ------------------------------------------------------------------
    # Tool execution
    # ------------------------------------------------------------------
    def call_tool(self, name: str, arguments: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        args = dict(arguments or {})

        if name == "agent_brain_register_topic":
            return self._register_topic(args)

        if name == "agent_brain_crawl_topic":
            return self._crawl_topic(args)

        if name == "agent_brain_scrape_topic":
            return self._scrape_topic(args)

        if name == "agent_brain_list_topics":
            return self._list_topics()

        if name == "agent_brain_list_pages":
            return self._list_pages(args)

        if name == "agent_brain_list_artifacts":
            return self._list_artifacts(args)

        if name == "agent_brain_get_artifact":
            return self._get_artifact(args)

        if name == "agent_brain_qdrant_search":
            return self._qdrant_search(args)

        if name == "agent_brain_qdrant_search_rrf":
            return self._qdrant_search_rrf(args)

        if name == "agent_brain_qdrant_stats":
            return self._qdrant_stats()

        if name == "agent_brain_qdrant_start":
            return self._qdrant_start()

        if name == "agent_brain_qdrant_stop":
            return self._qdrant_stop()

        if name == "agent_brain_search_results":
            return self._search_results(args)

        if name == "agent_brain_mark_seen":
            return self._mark_seen(args)

        if name == "agent_brain_mark_all_seen":
            return self._mark_all_seen()

        if name == "agent_brain_run_search":
            return self._run_search(args)

        if name == "agent_brain_health":
            return self._health()

        if name == "agent_model_list":
            return self._model_list()

        if name == "agent_model_load":
            return self._model_load(args)

        if name == "agent_model_unload":
            return self._model_unload()

        if name == "agent_model_active":
            return self._model_active()

        if name == "agent_model_run":
            return self._model_run(args)

        if name == "agent_audit_append":
            return self._audit_append(args)

        if name == "agent_audit_verify":
            return self._audit_verify(args)

        if name == "agent_audit_entries":
            return self._audit_entries(args)

        if name == "agent_resilience_state":
            return self._resilience_state(args)

        if name == "agent_feature_flag":
            return self._feature_flag(args)

        if name == "agent_secret_get":
            return self._secret_get(args)

        if name == "agent_webhook_enqueue":
            return self._webhook_enqueue(args)

        if name == "agent_webhook_process":
            return self._webhook_process(args)

        if name == "agent_email_send":
            return self._email_send(args)

        if name == "agent_email_send_sms":
            return self._email_send_sms(args)

        if name == "agent_email_template_write":
            return self._email_template_write(args)

        if name == "agent_email_template_list":
            return self._email_template_list(args)

        if name == "agent_email_template_get":
            return self._email_template_get(args)

        if name == "agent_email_template_delete":
            return self._email_template_delete(args)

        if name == "agent_email_template_search":
            return self._email_template_search(args)

        if name == "agent_email_template_render":
            return self._email_template_render(args)

        raise ValueError(f"Unknown MCP tool: {name!r}")

    # ------------------------------------------------------------------
    # Resource reading
    # ------------------------------------------------------------------
    def read_resource(self, uri: str) -> Dict[str, Any]:
        if uri == "agent_brain://topics":
            data = self._list_topics()
        elif uri == "agent_brain://settings":
            data = self._settings()
        elif uri == "agent_brain://ops/audit":
            if self._audit is None:
                data = {"error": "audit log not available"}
            else:
                data = {
                    "agent_ids": self._audit.agent_ids(),
                    "entries": {
                        aid: self._audit.entries(aid) for aid in self._audit.agent_ids()
                    },
                }
        elif uri == "agent_brain://ops/resilience":
            if self._resilience is None:
                data = {"error": "resilience not available"}
            else:
                data = {
                    "agent_ids": self._resilience.agent_ids(),
                    "states": {
                        aid: self._resilience.breaker_state(aid)
                        for aid in self._resilience.agent_ids()
                    },
                }
        elif uri == "agent_brain://ops/feature_flags":
            if self._feature_flags is None:
                data = {"error": "feature flags not available"}
            else:
                flags = self._feature_flags.list()
                data = {
                    "count": len(flags),
                    "flags": [f.to_dict() for f in flags],
                }
        elif uri == "agent_brain://email/templates":
            if self._templates is None:
                data = {"error": "email templates not available"}
            else:
                data = {"count": len(self._templates.list_templates()), "templates": self._templates.list_templates()}
        else:
            raise ValueError(f"Unknown MCP resource URI: {uri!r}")

        return {
            "contents": [
                {
                    "uri": uri,
                    "mimeType": "application/json",
                    "text": json.dumps(data, indent=2, default=str),
                }
            ]
        }

    def get_prompt(self, name: str, arguments: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        args = dict(arguments or {})
        if name == "trace_topic_pipeline":
            topic_id = str(args.get("topic_id", ""))
            pages = self._list_pages({"topic_id": topic_id})
            artifacts = self._list_artifacts({"topic_id": topic_id})
            stats = self._qdrant_stats()
            return {
                "description": f"Full scrape -> ETL -> Qdrant pipeline trace for topic {topic_id}",
                "messages": [
                    {
                        "role": "user",
                        "content": {
                            "type": "text",
                            "text": (
                                f"Pipeline trace for topic '{topic_id}':\n\n"
                                f"Pages:\n{json.dumps(pages, indent=2, default=str)}\n\n"
                                f"Artifacts:\n{json.dumps(artifacts, indent=2, default=str)}\n\n"
                                f"Qdrant Stats:\n{json.dumps(stats, indent=2, default=str)}"
                            ),
                        },
                    }
                ],
            }
        raise ValueError(f"Unknown MCP prompt: {name!r}")

    # ------------------------------------------------------------------
    # Internal implementations
    # ------------------------------------------------------------------
    def _run_async(self, coro):
        """Run a coroutine, handling both sync and async contexts."""
        import concurrent.futures

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running loop - safe to use asyncio.run
            return asyncio.run(coro)

        # We're inside a running event loop - run the coroutine in a
        # separate thread with its own event loop.
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(asyncio.run, coro)
            return future.result()

    def _register_topic(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if self._orchestrator is None:
            return {"error": "orchestrator not available"}

        async def _run():
            return await self._orchestrator.register_topic(
                name=str(args["name"]),
                url=str(args["url"]),
                scrape_interval_seconds=int(
                    args.get("scrape_interval_seconds", 3600)
                ),
                crawl=bool(args.get("crawl", True)),
                max_pages=int(args.get("max_pages", 20)),
                ignore_robots=bool(args.get("ignore_robots", False)),
            )

        topic = self._run_async(_run())
        return topic.model_dump(mode="json")

    def _crawl_topic(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if self._orchestrator is None:
            return {"error": "orchestrator not available"}
        topic_id = str(args["topic_id"])
        topics = {t.topic_id: t for t in self._orchestrator.list_topics()}
        if topic_id not in topics:
            return {"error": f"Topic {topic_id} not found"}

        async def _run():
            return await self._orchestrator.crawl_topic(topics[topic_id])

        return self._run_async(_run())

    def _scrape_topic(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if self._orchestrator is None:
            return {"error": "orchestrator not available"}
        topic_id = str(args["topic_id"])
        topics = {t.topic_id: t for t in self._orchestrator.list_topics()}
        if topic_id not in topics:
            return {"error": f"Topic {topic_id} not found"}

        async def _run():
            return await self._orchestrator.scrape_topic(topics[topic_id])

        return self._run_async(_run())

    def _list_topics(self) -> Dict[str, Any]:
        if self._orchestrator is None:
            return {"topics": [], "error": "orchestrator not available"}
        topics = self._orchestrator.list_topics()
        return {"topics": [t.model_dump(mode="json") for t in topics]}

    def _list_pages(self, args: Dict[str, Any]) -> Dict[str, Any]:
        topic_id = str(args["topic_id"])
        pages = []
        if self._brain_store is not None:
            try:
                for path in self._brain_store.list_files("pages"):
                    if path.endswith(".json"):
                        data = self._brain_store.read_json(path)
                        if data.get("topic_id") == topic_id:
                            pages.append(data)
            except Exception:
                pass
        return {"topic_id": topic_id, "pages": pages, "count": len(pages)}

    def _list_artifacts(self, args: Dict[str, Any]) -> Dict[str, Any]:
        topic_id = str(args["topic_id"])
        artifacts = []
        if self._brain_store is not None:
            try:
                for path in self._brain_store.list_files("artifacts"):
                    if path.endswith(".json"):
                        data = self._brain_store.read_json(path)
                        if data.get("topic_id") == topic_id:
                            artifacts.append(data)
            except Exception:
                pass
        return {"topic_id": topic_id, "artifacts": artifacts, "count": len(artifacts)}

    def _get_artifact(self, args: Dict[str, Any]) -> Dict[str, Any]:
        artifact_id = str(args["artifact_id"])
        if self._brain_store is None:
            return {"error": "brain store not available"}
        try:
            meta = self._brain_store.read_json(f"artifacts/{artifact_id}.json")
            path = meta.get("stored_path", "")
            if path.endswith(".csv"):
                return {
                    "artifact_id": artifact_id,
                    "format": "csv",
                    "content": self._brain_store.read_text(path),
                    "meta": meta,
                }
            return {
                "artifact_id": artifact_id,
                "format": "json",
                "content": self._brain_store.read_json(path),
                "meta": meta,
            }
        except Exception as exc:
            return {"error": str(exc)}

    def _qdrant_search(self, args: Dict[str, Any]) -> Dict[str, Any]:
        query = str(args["query"])
        top_k = int(args.get("top_k", 5))

        if self._qdrant is None or self._qdrant._client is None:
            return {"error": "qdrant pipeline not initialized", "results": []}

        try:
            from qdrant_client.models import Filter, FieldCondition, MatchValue, Range
            import numpy as np

            from applications.agent_brain.ingest.doc_pipeline.embed import TfidfEmbedder

            client = self._qdrant._client
            collection = self._qdrant.config.qdrant_collection
            vector_size = self._qdrant.config.vector_size

            # Build Qdrant filter from structured field args
            must_conditions = []
            salary_min = args.get("salary_min_usd")
            if salary_min is not None:
                must_conditions.append(
                    FieldCondition(key="salary_min_usd", range=Range(gte=float(salary_min)))
                )
            location = args.get("location")
            if location:
                must_conditions.append(
                    FieldCondition(key="location", match=MatchValue(value=location))
                )
            remote = args.get("remote")
            if remote is not None:
                must_conditions.append(
                    FieldCondition(key="remote", match=MatchValue(value=remote))
                )
            drug_testing = args.get("drug_testing")
            if drug_testing is not None:
                must_conditions.append(
                    FieldCondition(key="drug_testing", match=MatchValue(value=drug_testing))
                )
            portfolio_required = args.get("portfolio_required")
            if portfolio_required is not None:
                must_conditions.append(
                    FieldCondition(
                        key="portfolio_required", match=MatchValue(value=portfolio_required)
                    )
                )

            scroll_filter = Filter(must=must_conditions) if must_conditions else None

            # Fetch points (with optional filter) to build a consistent TF-IDF vocab
            records, _ = client.scroll(
                collection_name=collection,
                limit=10000,
                with_payload=True,
                with_vectors=True,
                scroll_filter=scroll_filter,
            )
            if not records:
                return {
                    "query": query,
                    "collection": collection,
                    "count": 0,
                    "results": [],
                    "filter_applied": bool(must_conditions),
                }

            texts = [r.payload.get("text", "") for r in records if r.payload.get("text")]
            if not texts:
                return {
                    "query": query,
                    "collection": collection,
                    "count": 0,
                    "results": [],
                    "filter_applied": bool(must_conditions),
                }

            embedder = TfidfEmbedder()
            embedder.fit(texts)
            q_vec = embedder.embed(query)

            # Pad query vector to match stored vector_size
            q_arr = np.asarray(q_vec, dtype=np.float64)
            if q_arr.size < vector_size:
                q_arr = np.pad(q_arr, (0, vector_size - q_arr.size))
            else:
                q_arr = q_arr[:vector_size]

            # Compute cosine similarity against all stored vectors
            scored = []
            for r in records:
                vec = r.vector
                if vec is None:
                    continue
                v = np.asarray(vec, dtype=np.float64)
                denom = (np.linalg.norm(q_arr) * np.linalg.norm(v)) or 1.0
                sim = float(np.dot(q_arr, v) / denom)
                scored.append((sim, r))

            scored.sort(key=lambda x: x[0], reverse=True)
            results = []
            for sim, r in scored[:top_k]:
                results.append({
                    "id": str(r.id),
                    "score": round(sim, 4),
                    "payload": r.payload or {},
                })

            return {
                "query": query,
                "collection": collection,
                "count": len(results),
                "results": results,
                "filter_applied": bool(must_conditions),
            }
        except Exception as exc:
            traceback.print_exc()
            return {"error": str(exc), "results": []}

    def _qdrant_search_rrf(self, args: Dict[str, Any]) -> Dict[str, Any]:
        queries = args.get("queries") or []
        if not isinstance(queries, list) or not queries:
            return {"error": "queries must be a non-empty list", "results": []}
        top_k = int(args.get("top_k", 10))
        k = int(args.get("k", 60))

        if self._qdrant is None or self._qdrant._client is None:
            return {"error": "qdrant pipeline not initialized", "results": []}

        try:
            from qdrant_client.models import Filter, FieldCondition, MatchValue, Range
            import numpy as np

            from applications.agent_brain.ingest.doc_pipeline.embed import TfidfEmbedder

            client = self._qdrant._client
            collection = self._qdrant.config.qdrant_collection
            vector_size = self._qdrant.config.vector_size

            # Build Qdrant filter from structured field args
            must_conditions = []
            salary_min = args.get("salary_min_usd")
            if salary_min is not None:
                must_conditions.append(
                    FieldCondition(key="salary_min_usd", range=Range(gte=float(salary_min)))
                )
            location = args.get("location")
            if location:
                must_conditions.append(
                    FieldCondition(key="location", match=MatchValue(value=location))
                )
            remote = args.get("remote")
            if remote is not None:
                must_conditions.append(
                    FieldCondition(key="remote", match=MatchValue(value=remote))
                )
            drug_testing = args.get("drug_testing")
            if drug_testing is not None:
                must_conditions.append(
                    FieldCondition(key="drug_testing", match=MatchValue(value=drug_testing))
                )
            portfolio_required = args.get("portfolio_required")
            if portfolio_required is not None:
                must_conditions.append(
                    FieldCondition(
                        key="portfolio_required", match=MatchValue(value=portfolio_required)
                    )
                )

            scroll_filter = Filter(must=must_conditions) if must_conditions else None

            # Fetch all points (with optional filter) to build a consistent TF-IDF vocab
            records, _ = client.scroll(
                collection_name=collection,
                limit=10000,
                with_payload=True,
                with_vectors=True,
                scroll_filter=scroll_filter,
            )
            if not records:
                return {
                    "queries": queries,
                    "k": k,
                    "top_k": top_k,
                    "count": 0,
                    "results": [],
                    "per_query_results": [],
                    "filter_applied": bool(must_conditions),
                }

            texts = [r.payload.get("text", "") for r in records if r.payload.get("text")]
            if not texts:
                return {
                    "queries": queries,
                    "k": k,
                    "top_k": top_k,
                    "count": 0,
                    "results": [],
                    "per_query_results": [],
                    "filter_applied": bool(must_conditions),
                }

            embedder = TfidfEmbedder()
            embedder.fit(texts)

            # Build vector index for all points
            point_vectors = []
            for r in records:
                vec = r.vector
                if vec is None:
                    continue
                v = np.asarray(vec, dtype=np.float64)
                if v.size < vector_size:
                    v = np.pad(v, (0, vector_size - v.size))
                else:
                    v = v[:vector_size]
                point_vectors.append((r, v))

            # Run each query and collect per-query rankings
            rrf_scores: dict[str, float] = {}
            point_by_id: dict[str, Any] = {}
            per_query_results: list[list[tuple[str, float]]] = []

            for qi, query in enumerate(queries):
                q_vec = embedder.embed(query)
                q_arr = np.asarray(q_vec, dtype=np.float64)
                if q_arr.size < vector_size:
                    q_arr = np.pad(q_arr, (0, vector_size - q_arr.size))
                else:
                    q_arr = q_arr[:vector_size]

                scored = []
                for r, v in point_vectors:
                    denom = (np.linalg.norm(q_arr) * np.linalg.norm(v)) or 1.0
                    sim = float(np.dot(q_arr, v) / denom)
                    scored.append((sim, r))
                scored.sort(key=lambda x: x[0], reverse=True)

                ranked = [(str(r.id), round(sim, 4)) for sim, r in scored[:top_k]]
                per_query_results.append(ranked)

                # Apply RRF: score += 1 / (k + rank) where rank is 1-indexed
                for rank, (pid, sim) in enumerate(ranked, start=1):
                    rrf_scores[pid] = rrf_scores.get(pid, 0.0) + 1.0 / (k + rank)
                    point_by_id[pid] = r

            # Sort by fused RRF score descending
            fused = sorted(rrf_scores.items(), key=lambda kv: kv[1], reverse=True)

            results = []
            for pid, rrf_score in fused[:top_k]:
                r = point_by_id[pid]
                results.append({
                    "id": pid,
                    "rrf_score": round(rrf_score, 6),
                    "payload": r.payload or {},
                })

            return {
                "queries": queries,
                "k": k,
                "top_k": top_k,
                "count": len(results),
                "results": results,
                "per_query_results": per_query_results,
                "filter_applied": bool(must_conditions),
            }
        except Exception as exc:
            traceback.print_exc()
            return {"error": str(exc), "results": []}

    def _qdrant_stats(self) -> Dict[str, Any]:
        if self._qdrant is None:
            return {"error": "qdrant pipeline not available"}
        return self._qdrant.stats()

    def _qdrant_start(self) -> Dict[str, Any]:
        if self._qdrant is None:
            return {"error": "qdrant pipeline not available"}

        async def _run():
            await self._qdrant.start_async()
            return {"status": "started"}

        try:
            return self._run_async(_run())
        except RuntimeError:
            return {"status": "already_running"}

    def _qdrant_stop(self) -> Dict[str, Any]:
        if self._qdrant is None:
            return {"error": "qdrant pipeline not available"}

        async def _run():
            await self._qdrant.stop()
            return {"status": "stopped"}

        return self._run_async(_run())

    def _search_results(self, args: Dict[str, Any]) -> Dict[str, Any]:
        term = args.get("term")
        unseen_only = args.get("unseen_only", False)
        results = []
        try:
            for path in self._brain_store.list_files("searches"):
                if path.endswith(".json"):
                    data = self._brain_store.read_json(path)
                    if term and data.get("term") != term:
                        continue
                    if unseen_only and not data.get("new", False):
                        continue
                    results.append(data)
        except Exception:
            pass
        results.sort(key=lambda x: x.get("searched_at", ""), reverse=True)
        return {"results": results, "count": len(results)}

    def _mark_seen(self, args: Dict[str, Any]) -> Dict[str, Any]:
        search_id = str(args["search_id"])
        try:
            data = self._brain_store.read_json(f"searches/{search_id}.json")
            data["seen"] = True
            data["new"] = False
            self._brain_store.save_json(f"searches/{search_id}.json", data)
            return {"status": "ok", "search_id": search_id}
        except Exception:
            return {"error": "search result not found"}

    def _mark_all_seen(self, args: Dict[str, Any]) -> Dict[str, Any]:
        count = 0
        try:
            for path in self._brain_store.list_files("searches"):
                if path.endswith(".json"):
                    data = self._brain_store.read_json(path)
                    if not data.get("seen", False):
                        data["seen"] = True
                        data["new"] = False
                        self._brain_store.save_json(path, data)
                        count += 1
        except Exception:
            pass
        return {"status": "ok", "marked": count}

    def _run_search(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if self._orchestrator is None:
            return {"error": "orchestrator not available"}

        async def _run():
            # Import the search function from main module
            from applications.agent_brain.main import _run_periodic_search
            await _run_periodic_search()
            return {"status": "ok"}

        try:
            return self._run_async(_run())
        except Exception as exc:
            return {"error": str(exc)}

    def _health(self) -> Dict[str, Any]:
        topics = self._orchestrator.list_topics() if self._orchestrator else []
        stats = self._qdrant_stats() if self._qdrant else {}
        return {
            "status": "ok",
            "topics": len(topics),
            "qdrant_stats": stats,
        }

    def _model_list(self) -> Dict[str, Any]:
        if self._harness is None:
            return {"error": "model harness not available"}
        return {"models": self._harness.list_models(), "count": self._harness.model_count()}

    def _model_load(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if self._harness is None:
            return {"error": "model harness not available"}
        agent_id = str(args.get("agent_id", "default"))
        model_id = str(args["model_id"])
        try:
            loaded = self._harness.load_model(agent_id, model_id)
            return {"loaded": loaded, "agent_id": agent_id}
        except Exception as exc:
            return {"error": str(exc)}

    def _model_unload(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if self._harness is None:
            return {"error": "model harness not available"}
        agent_id = str(args.get("agent_id", "default"))
        unloaded = self._harness.unload_model(agent_id)
        return {"unloaded": unloaded, "agent_id": agent_id}

    def _model_active(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if self._harness is None:
            return {"error": "model harness not available"}
        agent_id = str(args.get("agent_id", "default"))
        active = self._harness.active_model(agent_id)
        return {"active": active, "agent_id": agent_id}

    def _model_run(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if self._harness is None:
            return {"error": "model harness not available"}
        agent_id = str(args.get("agent_id", "default"))
        action = str(args.get("action", "info"))
        input_data = args.get("input_data")
        return self._harness.run_action(agent_id, action, input_data)

    # ------------------------------------------------------------------
    # Audit handlers
    # ------------------------------------------------------------------
    def _audit_append(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if self._audit is None:
            return {"error": "audit log not available"}
        entry = self._audit.append(
            agent_id=str(args.get("agent_id", "default")),
            actor=str(args["actor"]),
            action=str(args["action"]),
            resource=str(args["resource"]),
            details=args.get("details", {}),
        )
        return {"entry": entry.to_dict()}

    def _audit_verify(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if self._audit is None:
            return {"error": "audit log not available"}
        agent_id = str(args.get("agent_id", "default"))
        return {"agent_id": agent_id, "valid": self._audit.verify(agent_id)}

    def _audit_entries(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if self._audit is None:
            return {"error": "audit log not available"}
        agent_id = str(args.get("agent_id", "default"))
        return {"agent_id": agent_id, "entries": self._audit.entries(agent_id)}

    # ------------------------------------------------------------------
    # Resilience handlers
    # ------------------------------------------------------------------
    def _resilience_state(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if self._resilience is None:
            return {"error": "resilience not available"}
        agent_id = str(args.get("agent_id", "default"))
        return {"agent_id": agent_id, "state": self._resilience.breaker_state(agent_id)}

    # ------------------------------------------------------------------
    # Feature flag handlers
    # ------------------------------------------------------------------
    def _feature_flag(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if self._feature_flags is None:
            return {"error": "feature flags not available"}
        name = str(args["name"])
        user_id = args.get("user_id")
        enabled = self._feature_flags.is_enabled(name=name, user_id=user_id)
        return {"name": name, "enabled": enabled, "user_id": user_id}

    # ------------------------------------------------------------------
    # Secrets handlers
    # ------------------------------------------------------------------
    def _secret_get(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if self._secrets is None:
            return {"error": "secrets not available"}
        name = str(args["name"])
        value = self._secrets.get(name)
        return {"name": name, "found": value is not None}

    # ------------------------------------------------------------------
    # Webhook handlers
    # ------------------------------------------------------------------
    def _webhook_enqueue(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if self._webhook is None:
            return {"error": "webhook queue not available"}
        delivery = self._webhook.enqueue(
            url=str(args["url"]),
            event=str(args["event"]),
            payload=args.get("payload", {}),
            max_attempts=int(args.get("max_attempts", 3)),
        )
        return {
            "delivery_id": delivery.id,
            "url": delivery.url,
            "event": delivery.event,
            "status": delivery.status.value,
        }

    def _webhook_process(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if self._webhook is None:
            return {"error": "webhook queue not available"}

        async def _run():
            return await self._webhook.process_pending()

        return self._run_async(_run())

    # ------------------------------------------------------------------
    # Email handlers
    # ------------------------------------------------------------------
    def _email_send(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if self._email is None:
            return {"error": "email sender not available"}
        to = str(args["to"])
        template_name = args.get("template_name")
        template_vars = args.get("template_vars", {}) or {}
        try:
            if template_name:
                template = self._templates.get_template(template_name) if self._templates else None
                if template is None:
                    return {"error": f"Template not found: {template_name}"}
                rendered = self._templates.render_template(template_name, template_vars)
                return self._email.send_templated(
                    to,
                    rendered["subject"],
                    rendered["body"],
                    template_vars,
                    from_name=args.get("from_name"),
                )
            return self._email.send(
                to,
                str(args.get("subject", "")),
                str(args.get("body", "")),
                from_name=args.get("from_name"),
            )
        except Exception as exc:
            traceback.print_exc()
            return {"error": str(exc)}

    def _email_send_sms(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if self._email is None:
            return {"error": "email sender not available"}
        to = str(args["to"])
        body = str(args.get("body", ""))
        template_body = args.get("template_body")
        template_vars = args.get("template_vars", {}) or {}
        if template_body:
            body = self._email._render(template_body, template_vars)
        try:
            return self._email.send_sms(
                to,
                body,
                subject=args.get("subject"),
                from_name=args.get("from_name"),
            )
        except Exception as exc:
            traceback.print_exc()
            return {"error": str(exc)}

    def _email_template_write(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if self._templates is None:
            return {"error": "template store not available"}
        try:
            result = self._templates.write_template(
                template_id=str(args["template_id"]),
                name=str(args["name"]),
                category=str(args["category"]),
                subject=str(args["subject"]),
                body=str(args["body"]),
                variables=args.get("variables", []) or [],
            )
            return {"status": "written", "template": result}
        except Exception as exc:
            traceback.print_exc()
            return {"error": str(exc)}

    def _email_template_list(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if self._templates is None:
            return {"error": "template store not available"}
        return {"count": len(self._templates.list_templates()), "templates": self._templates.list_templates()}

    def _email_template_get(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if self._templates is None:
            return {"error": "template store not available"}
        template = self._templates.get_template(str(args["template_id"]))
        if template is None:
            return {"error": f"Template not found: {args['template_id']}"}
        return {"template": template}

    def _email_template_delete(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if self._templates is None:
            return {"error": "template store not available"}
        deleted = self._templates.delete_template(str(args["template_id"]))
        return {"deleted": deleted}

    def _email_template_search(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if self._templates is None:
            return {"error": "template store not available"}
        query = str(args["query"])
        top_k = int(args.get("top_k", 5))
        try:
            results = self._templates.search_templates(query, top_k=top_k)
            return {"query": query, "count": len(results), "results": results}
        except Exception as exc:
            traceback.print_exc()
            return {"error": str(exc), "results": []}

    def _email_template_render(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if self._templates is None:
            return {"error": "template store not available"}
        try:
            rendered = self._templates.render_template(
                str(args["template_id"]), args.get("template_vars", {}) or {}
            )
            return {"rendered": rendered}
        except Exception as exc:
            traceback.print_exc()
            return {"error": str(exc)}

    def _settings(self) -> Dict[str, Any]:
        from applications.agent_brain.core.settings import all as all_settings
        return all_settings()

    # ------------------------------------------------------------------
    # JSON-RPC 2.0 handler (stdio + FastAPI compatible)
    # ------------------------------------------------------------------
    def _handle_single_rpc(self, request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        method = str(request.get("method", ""))
        req_id = request.get("id")
        params = request.get("params") or {}

        if req_id is None and method.startswith("notifications/"):
            return None

        try:
            if method == "initialize":
                client_proto = str(params.get("protocolVersion") or DEFAULT_MCP_PROTOCOL_VERSION)
                result = {
                    "protocolVersion": client_proto,
                    "capabilities": {
                        "tools": {"listChanged": False},
                        "resources": {"subscribe": False, "listChanged": False},
                        "prompts": {"listChanged": False},
                    },
                    "serverInfo": {
                        "name": MCP_SERVER_NAME,
                        "version": MCP_SERVER_VERSION,
                    },
                }
            elif method == "ping":
                result = {}
            elif method == "tools/list":
                result = {"tools": self.get_tools_manifest()}
            elif method == "tools/call":
                tool_name = str(params.get("name", ""))
                tool_args = params.get("arguments") or {}
                payload = self.call_tool(tool_name, tool_args)
                result = {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(payload, indent=2, default=str),
                        }
                    ],
                    "structuredContent": payload,
                    "isError": False,
                }
            elif method == "resources/list":
                result = {"resources": self.get_resources_manifest()}
            elif method == "resources/read":
                uri = str(params.get("uri", ""))
                result = self.read_resource(uri)
            elif method == "prompts/list":
                result = {"prompts": self.get_prompts_manifest()}
            elif method == "prompts/get":
                prompt_name = str(params.get("name", ""))
                prompt_args = params.get("arguments") or {}
                result = self.get_prompt(prompt_name, prompt_args)
            else:
                if req_id is None:
                    return None
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {
                        "code": -32601,
                        "message": f"Method not found: {method}",
                    },
                }

            if req_id is None:
                return None
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": result,
            }
        except Exception as exc:
            traceback.print_exc()
            if req_id is None:
                return None
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {
                    "code": -32603,
                    "message": f"Internal error: {exc}",
                },
            }

    def handle_rpc(self, request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Handle a JSON-RPC request (single or batch)."""
        if isinstance(request, list):
            responses = []
            for req in request:
                resp = self._handle_single_rpc(req)
                if resp is not None:
                    responses.append(resp)
            return responses if responses else None
        return self._handle_single_rpc(request)
