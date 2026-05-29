"""Agent configuration loaded from environment variables (same .env as the stack)."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from repo root so the agent works when run from any directory.
_repo_root = Path(__file__).resolve().parent.parent
load_dotenv(_repo_root / ".env", override=False)


class AgentSettings:
    # ── Slack ──────────────────────────────────────────────────────────────────
    # xoxb-... bot token (OAuth & Permissions → Bot User OAuth Token)
    slack_bot_token: str = os.environ.get("SLACK_BOT_TOKEN", "")
    # xapp-... app-level token (Socket Mode → App-Level Tokens)
    slack_app_token: str = os.environ.get("SLACK_APP_TOKEN", "")

    # ── Airflow REST API ───────────────────────────────────────────────────────
    # Host-side URL; agent runs on the host alongside `make up`.
    airflow_api_url: str = os.environ.get("AIRFLOW_API_URL", "http://localhost:8080")
    airflow_api_user: str = os.environ.get(
        "AIRFLOW_API_USER",
        os.environ.get("AIRFLOW_ADMIN_USER", "admin"),
    )
    airflow_api_password: str = os.environ.get(
        "AIRFLOW_API_PASSWORD",
        os.environ.get("AIRFLOW_ADMIN_PASSWORD", "admin"),
    )
    airflow_dag_id: str = os.environ.get("AIRFLOW_DAG_ID", "rico_pipeline")

    # ── Ollama (reuse pipeline stack values) ───────────────────────────────────
    # Agent runs on host so endpoint must be host-accessible (localhost:11434).
    ollama_endpoint: str = os.environ.get(
        "AGENT_OLLAMA_ENDPOINT",
        # Fall back to OLLAMA_ENDPOINT but swap container hostname with localhost.
        os.environ.get("OLLAMA_ENDPOINT", "http://localhost:11434").replace(
            "http://ollama:", "http://localhost:"
        ),
    )
    ollama_model: str = os.environ.get("OLLAMA_MODEL", "qwen2.5:3b")
    llm_timeout: int = int(os.environ.get("AGENT_LLM_TIMEOUT", "60"))

    def validate(self) -> None:
        missing = []
        if not self.slack_bot_token:
            missing.append("SLACK_BOT_TOKEN")
        if not self.slack_app_token:
            missing.append("SLACK_APP_TOKEN")
        if missing:
            raise RuntimeError(
                f"Missing required env vars: {', '.join(missing)}\n"
                "Set them in .env or export them before running `make agent`."
            )


settings = AgentSettings()
