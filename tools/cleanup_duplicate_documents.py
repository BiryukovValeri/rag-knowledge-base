#!/usr/bin/env python
"""
Очистка дублей документов и тестового документа в Supabase.

Удаляет:
  - customer-intellect
  - financial-intellect
  - leadership-intellect
  - test-document-001

Порядок:
  1) chunks по document_id
  2) sections по document_id
  3) documents по id

DRY_RUN управляет тем, реально ли удалять данные.
"""

import os
import sys
from pathlib import Path
from typing import List

# -----------------------------------------------------------
# Настройка PYTHONPATH: добавляем корень проекта в sys.path
# -----------------------------------------------------------

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent  # .../rag-knowledge-base

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Теперь импорт из app.* должен работать так же, как в ingest_*.py
from app.core.db import get_supabase_client  # type: ignore


# СЛАГИ, КОТОРЫЕ НУЖНО УДАЛИТЬ ПОЛНОСТЬЮ
DELETE_SLUGS: List[str] = [
    "customer-intellect",
    "financial-intellect",
    "leadership-intellect",
    "test-document-001",
]

# РЕЖИМ ПРОВЕРКИ:
#   True  – только показать, что БЫ удалили, без реального удаления
#   False – реально удалить
DRY_RUN: bool = False


def main() -> None:
    client = get_supabase_client()

    print("=== Cleanup duplicate/test documents ===")
    print(f"PROJECT_ROOT = {PROJECT_ROOT}")
    print(f"DRY_RUN      = {DRY_RUN}")
    print(f"TARGET SLUGS = {', '.join(DELETE_SLUGS)}")
    print()

    # 1. Найти все документы с указанными slug
    resp = (
        client.table("documents")
        .select("id, slug, title, version, status")
        .in_("slug", DELETE_SLUGS)
        .execute()
    )

    docs = resp.data or []
    if not docs:
        print("Нет документов с указанными slug. Нечего удалять.")
        return

    print("Найдены документы для удаления:")
    for d in docs:
        print(
            f"- id={d['id']}, slug={d['slug']}, "
            f"title={d.get('title')!r}, version={d.get('version')}, status={d.get('status')}"
        )
    print()

    if DRY_RUN:
        print("DRY_RUN = True → реальные DELETE-запросы НЕ выполняются.")
        return

    # 2. Удаление по каждому документу
    for d in docs:
        doc_id = d["id"]
        slug = d["slug"]
        title = d.get("title")

        print(f"--- Удаление документа slug={slug}, id={doc_id} ---")

        # 2.1 Удаляем chunks
        chunks_resp = (
            client.table("chunks")
            .delete()
            .eq("document_id", doc_id)
            .execute()
        )
        print(f"  chunks delete → data={chunks_resp.data} count={chunks_resp.count}")

        # 2.2 Удаляем sections
        sections_resp = (
            client.table("sections")
            .delete()
            .eq("document_id", doc_id)
            .execute()
        )
        print(
            f"  sections delete → data={sections_resp.data} count={sections_resp.count}"
        )

        # 2.3 Удаляем сам document
        doc_resp = (
            client.table("documents")
            .delete()
            .eq("id", doc_id)
            .execute()
        )
        print(f"  documents delete → data={doc_resp.data} count={doc_resp.count}")
        print(f"--- Готово для slug={slug}, title={title!r} ---\n")

    print("=== Cleanup finished ===")


if __name__ == "__main__":
    main()
