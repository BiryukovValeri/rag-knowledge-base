from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

from loguru import logger
from supabase import Client

from app.core.db import get_supabase_client
from ingest.extract_text.docx_reader import extract_blocks_from_docx
from ingest.chunking.chunker import (
    split_into_sections,
    build_text_chunks_for_section,
    Section,
)


def get_or_create_document_record(
    client: Client,
    slug: str,
    title: str,
    subtitle: str | None,
    series: str | None,
    doc_type: str,
    version: int,
    language: str,
) -> str:
    """
    Ищем документ по (slug, version).
    Если найден — возвращаем id.
    Если нет — создаём новый.
    """
    logger.info(f"Looking for existing document (slug={slug}, version={version})...")
    existing = (
        client.table("documents")
        .select("id")
        .eq("slug", slug)
        .eq("version", version)
        .limit(1)
        .execute()
    )

    if existing.data:
        document_id = existing.data[0]["id"]
        logger.info(f"Found existing document with id={document_id}")
        return document_id

    logger.info("Existing document not found. Creating new one...")
    doc_data = {
        "slug": slug,
        "title": title,
        "subtitle": subtitle,
        "series": series,
        "doc_type": doc_type,
        "version": version,
        "language": language,
        "status": "active",
    }
    resp = client.table("documents").insert(doc_data).execute()
    logger.info(f"Insert document response: {resp}")
    if not resp.data:
        raise RuntimeError("No data returned after inserting document")
    document_id = resp.data[0]["id"]
    logger.info(f"Created new document with id={document_id}")
    return document_id


def cleanup_existing_content(client: Client, document_id: str) -> None:
    """
    Удаляем старые chunks и sections для этого документа.
    """
    logger.info(f"Cleaning up existing chunks for document_id={document_id}...")
    resp_chunks = (
        client.table("chunks").delete().eq("document_id", document_id).execute()
    )
    logger.info(f"Chunks delete response: {resp_chunks}")

    logger.info(f"Cleaning up existing sections for document_id={document_id}...")
    resp_sections = (
        client.table("sections").delete().eq("document_id", document_id).execute()
    )
    logger.info(f"Sections delete response: {resp_sections}")


def insert_sections(
    client: Client,
    document_id: str,
    sections: List[Section],
) -> List[str]:
    """
    Вставляет H1-секции в таблицу sections.
    Возвращает список section_id в том же порядке.
    """

    # === FIX №1 ===
    # Если секций нет — НЕ ДЕЛАЕМ INSERT (Supabase падает на insert([]))
    if not sections:
        logger.warning(
            "No sections detected (0 H1 headings). "
            "Skipping insertion into 'sections' table."
        )
        return []

    logger.info(f"Inserting {len(sections)} sections into 'sections' table...")

    payload = []
    for s in sections:
        payload.append(
            {
                "document_id": document_id,
                "title": s.title,
                "level": 1,
                "order_index": s.index,
                "full_path": s.title,  # NOT NULL → используем заголовок как путь
            }
        )

    resp = client.table("sections").insert(payload).execute()
    logger.info(f"Insert sections response: {resp}")

    if not resp.data:
        raise RuntimeError("No data returned after inserting sections")

    section_ids: List[str] = [row["id"] for row in resp.data]
    return section_ids


def insert_chunks_for_book(
    client: Client,
    document_id: str,
    sections: List[Section],
    section_ids: List[str],
) -> None:
    """
    Для каждой секции генерирует текстовые чанки и пишет их в таблицу chunks.
    """
    if len(sections) != len(section_ids):
        raise ValueError("sections and section_ids length mismatch")

    logger.info("Building and inserting chunks for all sections...")

    all_chunks_payload = []
    global_chunk_index = 0

    for s, section_id in zip(sections, section_ids):
        text_chunks = build_text_chunks_for_section(s)
        if not text_chunks:
            continue

        for t in text_chunks:
            global_chunk_index += 1
            all_chunks_payload.append(
                {
                    "document_id": document_id,
                    "section_id": section_id,
                    "chunk_index": global_chunk_index,
                    "page_from": None,
                    "page_to": None,
                    "text": t,
                    "embedding": None,
                    "tokens_count": None,
                    "quality_flag": "ok",
                }
            )

    logger.info(f"Prepared {len(all_chunks_payload)} chunks to insert.")

    batch_size = 200
    for i in range(0, len(all_chunks_payload), batch_size):
        batch = all_chunks_payload[i : i + batch_size]
        logger.info(
            f"Inserting batch {i}..{i+len(batch)-1} of {len(all_chunks_payload)}"
        )
        resp = client.table("chunks").insert(batch).execute()
        logger.info(f"Insert batch response: {resp}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generic book ingestion into Supabase (documents/sections/chunks)."
    )
    parser.add_argument(
        "--file",
        required=True,
        help="Путь к DOCX-файлу книги (например: data/raw/Книга 1 ....docx)",
    )
    parser.add_argument(
        "--slug",
        required=True,
        help="Уникальный slug книги (латиницей, например: kniga-1-strategicheskiy-intellekt)",
    )
    parser.add_argument(
        "--title",
        required=True,
        help="Полное название книги для поля documents.title",
    )
    parser.add_argument(
        "--subtitle",
        required=False,
        default=None,
        help="Подзаголовок / описание книги (documents.subtitle)",
    )
    parser.add_argument(
        "--series",
        required=False,
        default=None,
        help="Серия (например: Интеллекты, Формула управления, IntlX)",
    )
    parser.add_argument(
        "--doc-type",
        required=False,
        default="книга",
        help='Тип документа (по умолчанию "книга")',
    )
    parser.add_argument(
        "--version",
        required=False,
        type=int,
        default=1,
        help="Версия книги (целое число, по умолчанию 1)",
    )
    parser.add_argument(
        "--language",
        required=False,
        default="ru",
        help='Язык книги (по умолчанию "ru")',
    )
    return parser.parse_args()


def ingest_book_generic() -> None:
    args = parse_args()

    docx_path = Path(args.file)
    if not docx_path.exists():
        raise FileNotFoundError(f"DOCX file not found: {docx_path}")

    logger.info(
        f"Starting ingestion for '{args.title}' "
        f"(slug={args.slug}, version={args.version})..."
    )

    client = get_supabase_client()

    # 1. Получаем / создаём документ
    document_id = get_or_create_document_record(
        client=client,
        slug=args.slug,
        title=args.title,
        subtitle=args.subtitle,
        series=args.series,
        doc_type=args.doc_type,
        version=args.version,
        language=args.language,
    )

    # 2. Чистим старое содержимое
    cleanup_existing_content(client, document_id)

    # 3. Читаем DOCX и режем на секции
    blocks = extract_blocks_from_docx(docx_path)
    sections = split_into_sections(blocks)
    logger.info(f"Book split into {len(sections)} sections (H1).")

    # 4. Вставляем секции
    section_ids = insert_sections(client, document_id, sections)
    logger.info(f"Inserted {len(section_ids)} sections.")

    # 5. Вставляем чанки
    insert_chunks_for_book(client, document_id, sections, section_ids)

    logger.info(f"Ingestion for '{args.title}' finished.")


if __name__ == "__main__":
    ingest_book_generic()
