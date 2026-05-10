"""Recall@5 self-test evaluation using stored embeddings."""

from __future__ import annotations

import logging

from pgvector.psycopg import register_vector

from rico_dag.db import get_conn, logger_with_run_id

_log = logging.getLogger(__name__)


def run(*, run_id: str) -> dict:
    log = logger_with_run_id(_log, run_id)
    results = {}

    with get_conn() as conn:
        register_vector(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT model_version, embedding_kind FROM screens_embeddings WHERE run_id = %s",
                (run_id,),
            )
            model_kinds = cur.fetchall()

            for model_version, embedding_kind in model_kinds:
                cur.execute(
                    "SELECT screen_id, vector FROM screens_embeddings WHERE run_id = %s AND model_version = %s AND embedding_kind = %s",
                    (run_id, model_version, embedding_kind),
                )
                rows = cur.fetchall()

                if not rows:
                    continue

                hits = 0
                for screen_id, vector in rows:
                    cur.execute(
                        """
                        SELECT screen_id FROM screens_embeddings
                        WHERE model_version = %s AND embedding_kind = %s
                        ORDER BY vector <-> %s
                        LIMIT 5
                        """,
                        (model_version, embedding_kind, vector),
                    )
                    top5 = [r[0] for r in cur.fetchall()]
                    if screen_id in top5:
                        hits += 1

                recall = hits / len(rows)
                cur.execute(
                    "INSERT INTO screens_eval (embedding_model_version, n_queries, recall_at_5) VALUES (%s, %s, %s)",
                    (model_version, len(rows), recall),
                )
                results[f"{model_version}/{embedding_kind}"] = recall
                log.info("recall@5 for %s/%s: %.2f (%d/%d)", model_version, embedding_kind, recall, hits, len(rows))

        conn.commit()

    log.info("eval complete: %s", results)
    return {"run_id": run_id, "task": "eval", "results": results}
