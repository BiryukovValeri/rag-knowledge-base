from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional, Tuple
import json

import numpy as np
from loguru import logger
from openai import OpenAI

from app.core.db import get_supabase_client

# ДОЛЖНО совпадать с моделью, используемой в ingest/pipeline/build_embeddings.py
EMBEDDING_MODEL = "text-embedding-3-small"


def get_openai_client() -> OpenAI:
    """
    Унифицированная точка получения клиента OpenAI.
    qa_cli импортирует именно эту функцию.
    """
    return OpenAI()


@dataclass
class Chunk:
    """
    Представление чанка, совместимое с тем, что ожидает qa_cli.
    """
    id: str
    document_id: str
    text: str
    # В Supabase embedding может быть:
    #  - list[float]
    #  - строка с JSON-массивом
    embedding: Optional[Any]
    quality_flag: str


def _get_supabase():
    return get_supabase_client()


def load_chunks(
    slug: Optional[str] = None,
    slugs: Optional[List[str]] = None,
    limit: int = 2000,
) -> List[Chunk]:
    """
    Загружает чанки из Supabase.

    Варианты фильтрации:
      - если задан slug — берём документы только с этим slug;
      - если задан slugs (список) — берём документы с любым из этих slug;
      - если ни slug, ни slugs не заданы — берём все документы.

    Во всех случаях:
      – embedding IS NOT NULL,
      – quality_flag == 'ok',
      – ограничение по limit.
    """
    supabase = _get_supabase()

    # Базовый запрос по чанкам
    query = (
        supabase.table("chunks")
        .select("id, document_id, text, embedding, quality_flag")
    )

    # Если указан slug или список slugs — сначала находим document_id по таблице documents
    doc_ids: List[str] = []

    if slugs:
        logger.info("Loading document_ids for slugs=%s", slugs)
        doc_resp = (
            supabase.table("documents")
            .select("id, slug")
            .in_("slug", slugs)
            .execute()
        )
        doc_rows = doc_resp.data or []
        doc_ids = [row["id"] for row in doc_rows if "id" in row]

        if not doc_ids:
            logger.warning("No documents found for slugs=%s", slugs)
            return []

    elif slug:
        logger.info("Loading document_ids for slug=%s", slug)
        doc_resp = (
            supabase.table("documents")
            .select("id")
            .eq("slug", slug)
            .execute()
        )
        doc_rows = doc_resp.data or []
        doc_ids = [row["id"] for row in doc_rows if "id" in row]

        if not doc_ids:
            logger.warning("No documents found for slug=%s", slug)
            return []

    # Если мы нашли какие-то document_id — фильтруем по ним чанки
    if doc_ids:
        query = query.in_("document_id", doc_ids)

    # Фильтрация: embedding IS NOT NULL и нормальный quality_flag
    query = (
        query
        .not_.is_("embedding", None)  # embedding IS NOT NULL
        .eq("quality_flag", "ok")
    )

    if limit is not None and limit > 0:
        query = query.limit(limit)

    resp = query.execute()
    rows = resp.data or []

    logger.info(
        "Loaded %d chunks (slug=%s, slugs=%s, limit=%s)",
        len(rows),
        slug,
        slugs,
        limit,
    )

    chunks: List[Chunk] = []
    for row in rows:
        chunks.append(
            Chunk(
                id=row["id"],
                document_id=row["document_id"],
                text=row["text"],
                embedding=row.get("embedding"),
                quality_flag=row.get("quality_flag", "ok"),
            )
        )

    return chunks


def embed_query(text: str) -> List[float]:
    """
    Строит эмбеддинг для текстового запроса через OpenAI.
    """
    client = get_openai_client()
    logger.info("Requesting embedding for query (len=%d chars)", len(text))
    resp = client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=[text],
    )
    return resp.data[0].embedding


def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """
    Косинусное сходство двух векторов.
    """
    denom = float(np.linalg.norm(a) * np.linalg.norm(b))
    if denom == 0.0:
        return 0.0
    return float(np.dot(a, b) / denom)


def _normalize_embedding(emb_raw: Any) -> Optional[List[float]]:
    """
    Приводим embedding к List[float].

    Возможные варианты:
      - уже list[float] → возвращаем как есть;
      - строка с JSON-массивом → json.loads(...);
      - всё остальное → None.
    """
    if emb_raw is None:
        return None

    # У Supabase часто vector хранится как строка с JSON
    if isinstance(emb_raw, str):
        try:
            parsed = json.loads(emb_raw)
        except json.JSONDecodeError:
            logger.warning("Failed to JSON-decode embedding string, skipping chunk")
            return None
        if not isinstance(parsed, list):
            return None
        return parsed

    if isinstance(emb_raw, (list, tuple)):
        return list(emb_raw)

    # Неподдерживаемый формат
    logger.warning("Unsupported embedding type %s, skipping chunk", type(emb_raw))
    return None


def score_chunks_by_similarity(
    query_embedding: List[float],
    chunks: List[Chunk],
) -> List[Tuple[Chunk, float]]:
    """
    Считает косинусное сходство между эмбеддингом запроса и эмбеддингами чанков.

    ВАЖНО: возвращает список (Chunk, score), а не только Chunk —
    это соответствует ожиданиям qa_cli:
        scored = retrieve_top_k(...)
        chunks = [ch for ch, _sim in scored]
    """
    q_vec = np.array(query_embedding, dtype=np.float32)

    scored: List[Tuple[Chunk, float]] = []

    for ch in chunks:
        emb_list = _normalize_embedding(ch.embedding)
        if not emb_list:
            # нет валидного эмбеддинга — пропускаем
            continue

        try:
            c_vec = np.array(emb_list, dtype=np.float32)
        except (TypeError, ValueError):
            logger.warning("Failed to convert embedding to np.array, skipping chunk")
            continue

        sim = _cosine_similarity(q_vec, c_vec)
        scored.append((ch, sim))

    # сортируем по score по убыванию
    scored_sorted = sorted(scored, key=lambda pair: pair[1], reverse=True)
    return scored_sorted


def retrieve_top_k(
    query: str,
    slug: Optional[str] = None,
    slugs: Optional[List[str]] = None,
    k: int = 5,
    preload_limit: int = 2000,
) -> List[Tuple[Chunk, float]]:
    """
    Главная функция retrieval-слоя, которую вызывает qa_cli и web-API.

    1) Загружает до preload_limit чанков (по slug, по списку slugs или по всем документам).
    2) Строит эмбеддинг запроса.
    3) Считает косинусное сходство с каждым чанком.
    4) Возвращает top-k (Chunk, score), отсортированные по score (убывание).
    """
    logger.info(
        "retrieve_top_k(query_len=%d, slug=%s, slugs=%s, k=%d, preload_limit=%d)",
        len(query),
        slug,
        slugs,
        k,
        preload_limit,
    )

    chunks = load_chunks(slug=slug, slugs=slugs, limit=preload_limit)
    if not chunks:
        logger.warning("No chunks loaded for slug=%s, slugs=%s", slug, slugs)
        return []

    query_embedding = embed_query(query)
    scored_chunks = score_chunks_by_similarity(query_embedding, chunks)

    top = scored_chunks[:k]
    logger.info("retrieve_top_k: got %d results", len(top))
    return top
