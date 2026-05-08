"""Centralized configuration for the RICO DAG."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str = os.getenv("POSTGRES_DB", "rico")
    postgres_user: str = os.getenv("POSTGRES_USER", "rico")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "rico")

    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    minio_bucket: str = os.getenv("MINIO_BUCKET", "rico-raw")

    ollama_endpoint: str = os.getenv("OLLAMA_ENDPOINT", "http://localhost:11434")
    ollama_model: str = os.getenv("OLLAMA_MODEL", "qwen2.5:3b")
    prompt_version: str = os.getenv("PROMPT_VERSION", "v1")

    clip_version: str = os.getenv("CLIP_VERSION", "ViT-B-32/laion2b_s34b_b79k")
    sbert_version: str = os.getenv("SBERT_VERSION", "all-MiniLM-L6-v2")

    chosen_screens_file: str = os.getenv("CHOSEN_SCREENS_FILE", "data/chosen_screens.txt")


settings = Settings()
