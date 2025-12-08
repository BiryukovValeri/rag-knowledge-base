#!/usr/bin/env python3
from __future__ import annotations

import sys
import yaml
import subprocess
from pathlib import Path
from typing import Any, Dict, List

from loguru import logger
from app.core.db import get_supabase_client


# ============================================================
# ЗАГРУЗКА МАНИФЕСТА
# ============================================================

def load_manifest(path: str = "data/ingest_manifest.yaml") -> List[Dict[str, Any]]:
    logger.info("Loading manifest from {}...", path)
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Manifest not found: {path}")

    data = yaml.safe_load(p.read_text(encoding="utf-8"))
    docs = data.get("documents", [])
    logger.info("Loaded {} document entries from manifest.", len(docs))
    return docs


# ============================================================
# ПРОВЕРКА НАЛИЧИЯ ФАЙЛОВ
# ============================================================

def check_files_exist(docs: List[Dict[str, Any]]) -> None:
    for d in docs:
        doc_id = d["id"]
        file_path = d["file"]
        if Path(file_path).exists():
            logger.info("[FILES] OK   – {}: {}", doc_id, file_path)
        else:
            logger.error("[FILES] MISSING – {}: {}", doc_id, file_path)
            raise FileNotFoundError(f"Missing file: {file_path}")


# ============================================================
# ПРОВЕРКА В SUPABASE
# ============================================================

def check_in_supabase(client, docs: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Возвращает dict:
       { slug: {"exists": bool, "id":..., "version":...} }
    """
    out = {}
    for d in docs:
        slug = d["slug"]
        version = d.get("version", 1) or 1
        logger.info("[SUPABASE] Checking document slug={}, version={}...", slug, version)

        resp = (
            client.table("documents")
            .select("*")
            .eq("slug", slug)
            .eq("version", version)
            .execute()
        )

        if resp.data:
            row = resp.data[0]
            logger.info(
                "[SUPABASE] EXISTS – {}: id={}, version={}, updated_at={}",
                d["id"],
                row["id"],
                row["version"],
                row["updated_at"],
            )
            out[slug] = {"exists": True, "record": row}
        else:
            logger.info("[SUPABASE] NEW    – {}: no record yet.", d["id"])
            out[slug] = {"exists": False, "record": None}

    return out


# ============================================================
# ЗАПУСК INGEST ПРОЦЕССА
# ============================================================

def run_ingest_for_doc(doc: Dict[str, Any]) -> None:
    """
    Запускает pipeline для одного документа.
    Гарантия: никаких None в команду не попадает.
    """

    doc_id = doc["id"]
    slug = doc["slug"]
    file_path = doc["file"]

    version = str(doc.get("version", 1) or 1)
    language = doc.get("language") or "ru"
    doc_type = (doc.get("doc_type") or "book").lower()

    module = "ingest.pipeline.ingest_book_generic"

    cmd: List[str] = [
        sys.executable,
        "-m",
        module,
        "--file",
        file_path,
        "--slug",
        slug,
        "--version",
        version,
        "--language",
        language,
    ]

    def add_opt(key: str, flag: str):
        val = doc.get(key)
        if val:
            cmd.extend([flag, val])

    add_opt("title", "--title")
    add_opt("subtitle", "--subtitle")
    add_opt("series", "--series")

    logger.info("[INGEST] Command: {}", " ".join(cmd))

    result = subprocess.run(cmd, check=True)
    logger.info(
        "[INGEST] Finished for {} (slug={}, version={}), returncode={}",
        doc_id,
        slug,
        version,
        result.returncode,
    )


# ============================================================
# ОСНОВНАЯ ФУНКЦИЯ
# ============================================================

def ingest_from_manifest(
    manifest_path: str,
    mode: str,
    ids: List[str] | None = None,
) -> None:

    logger.info("Starting ingest_from_manifest (mode={})...", mode)

    docs = load_manifest(manifest_path)

    if ids:
        filtered = []
        for d in docs:
            if d["id"] in ids or d["slug"] in ids:
                filtered.append(d)
        logger.info(
            "Filtered by ids/slugs: {} entries remaining (from {}).",
            len(filtered),
            len(ids),
        )
        docs = filtered

    check_files_exist(docs)

    client = get_supabase_client()
    supa_check = check_in_supabase(client, docs)

    if mode == "check":
        logger.info("Mode=check – ingestion is NOT executed (dry run).")
        return

    if mode == "ingest":
        new_docs = [d for d in docs if not supa_check[d["slug"]]["exists"]]
        logger.info(
            "Mode=ingest – starting ingestion for {} NEW documents (out of {}).",
            len(new_docs),
            len(docs),
        )

        for d in new_docs:
            run_ingest_for_doc(d)
        return

    raise ValueError(f"Invalid mode: {mode}")


# ============================================================
# ENTRY POINT
# ============================================================

def main():
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--manifest",
        default="data/ingest_manifest.yaml",
        help="Путь к ingest_manifest.yaml",
    )

    parser.add_argument(
        "--mode",
        choices=["check", "ingest"],
        required=True,
        help="Режим работы.",
    )

    parser.add_argument(
        "--ids",
        nargs="*",
        default=None,
        help="Ограничить запуск документами по их id или slug.",
    )

    args = parser.parse_args()

    ingest_from_manifest(
        manifest_path=args.manifest,
        mode=args.mode,
        ids=args.ids,
    )


if __name__ == "__main__":
    main()
