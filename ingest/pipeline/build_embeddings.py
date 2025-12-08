from __future__ import annotations

from typing import List, Dict, Any

from loguru import logger
from openai import OpenAI

from app.core.db import get_supabase_client


# -----------------------------
# Константы
# -----------------------------

# Размер вектора в БД = 1536 → используем совместимую модель
EMBEDDING_MODEL = "text-embedding-3-small"
BATCH_SIZE = 100  # размер батча


# -----------------------------
# Работа с БД
# -----------------------------

def fetch_chunks_without_embedding(limit: int = BATCH_SIZE) -> List[Dict[str, Any]]:
    """
    Берём чанки, у которых embedding = NULL.
    ВАЖНО: отбрасываем битые строки без document_id или без текста.
    Забираем все поля, чтобы при upsert не затирать их в NULL.
    """
    supabase = get_supabase_client()
    resp = (
        supabase.table("chunks")
        .select(
            "id, document_id, section_id, chunk_index, "
            "page_from, page_to, text, quality_flag, tokens_count"
        )
        .is_("embedding", None)
        .limit(limit)
        .execute()
    )

    raw_rows = resp.data or []

    # фильтрация битых строк
    rows = [
        r
        for r in raw_rows
        if r.get("document_id") is not None and r.get("text") not in (None, "")
    ]

    logger.info(
        "Fetched %d chunks without embedding (filtered from %d raw)",
        len(rows),
        len(raw_rows),
    )
    return rows


def update_embeddings(rows: List[Dict[str, Any]], embeddings: List[List[float]]) -> None:
    """
    Обновляем embedding через upsert.
    В payload передаём:
    - id (для однозначной идентификации строки),
    - document_id и остальные колонки, чтобы избежать вставки
      новой строки с NULL'ами, и чтобы не ломать NOT NULL.
    """
    if not rows:
        return

    if len(rows) != len(embeddings):
        raise ValueError(
            f"Rows count ({len(rows)}) != embeddings count ({len(embeddings)})"
        )

    supabase = get_supabase_client()

    payload: List[Dict[str, Any]] = []
    for row, emb in zip(rows, embeddings):
        row_id = row.get("id")
        if not row_id:
            logger.error("Chunk without id in rows: %s", row)
            continue

        payload.append(
            {
                # ключи / идентификаторы
                "id": row_id,
                "document_id": row.get("document_id"),
                "section_id": row.get("section_id"),
                "chunk_index": row.get("chunk_index"),

                # вспомогательные поля
                "page_from": row.get("page_from"),
                "page_to": row.get("page_to"),
                "text": row.get("text"),
                "quality_flag": row.get("quality_flag", "ok"),
                "tokens_count": row.get("tokens_count"),

                # новое поле embedding
                "embedding": emb,
            }
        )

    if not payload:
        return

    logger.info("Upserting %d chunk embeddings into 'chunks' table", len(payload))
    resp = supabase.table("chunks").upsert(payload).execute()
    logger.info("Upsert response: %s", resp.data)


# -----------------------------
# Embeddings через OpenAI
# -----------------------------

def build_embeddings_for_batch(client: OpenAI, rows: List[Dict[str, Any]]) -> None:
    """
    Один батч:
    1) вытаскиваем тексты;
    2) считаем embedding;
    3) пишем в БД.
    """
    if not rows:
        logger.info("No rows in batch, nothing to embed")
        return

    texts = [row["text"] or "" for row in rows]

    logger.info(
        "Requesting embeddings for %d texts (model=%s)",
        len(texts),
        EMBEDDING_MODEL,
    )

    response = client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=texts,
    )

    embeddings: List[List[float]] = [item.embedding for item in response.data]
    update_embeddings(rows, embeddings)


# -----------------------------
# Основной цикл пайплайна
# -----------------------------

def build_embeddings_for_all_chunks() -> None:
    """
    Цикл до полного обнуления очереди:
    - берём батч чанков без embedding;
    - считаем embedding;
    - пишем в БД;
    - повторяем, пока нечего обрабатывать.
    """
    client = OpenAI()

    total_processed = 0
    iteration = 0

    while True:
        iteration += 1
        logger.info("Iteration %d: fetching next batch...", iteration)
        rows = fetch_chunks_without_embedding(limit=BATCH_SIZE)

        if not rows:
            logger.info("No more *valid* chunks without embeddings. Finished.")
            break

        build_embeddings_for_batch(client, rows)
        batch_count = len(rows)
        total_processed += batch_count
        logger.info(
            "Iteration %d done. Batch size=%d, total processed=%d",
            iteration,
            batch_count,
            total_processed,
        )


# -----------------------------
# Точка входа
# -----------------------------

if __name__ == "__main__":
    logger.info("Starting embedding pipeline...")
    build_embeddings_for_all_chunks()
    logger.info("Embedding pipeline finished.")
