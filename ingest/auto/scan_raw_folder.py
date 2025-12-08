from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Any

import yaml

# Корень RAW-папки
RAW_ROOT = Path("data/raw")
# Путь к манифесту
MANIFEST_PATH = Path("data/ingest_manifest.yaml")

# Допустимые расширения
ALLOWED_SUFFIXES = {".docx", ".pptx", ".ppt", ".pdf"}

# Приоритет расширений в одной группе (stem)
PREFERRED_SUFFIX_ORDER = [".docx", ".pptx", ".ppt", ".pdf"]


def load_manifest() -> List[Dict[str, Any]]:
    """Загружаем список документов из ingest_manifest.yaml."""
    if not MANIFEST_PATH.exists():
        return []

    with MANIFEST_PATH.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    docs = data.get("documents", [])
    if not isinstance(docs, list):
        raise ValueError("Поле 'documents' в ingest_manifest.yaml должно быть списком.")
    return docs


def save_manifest(docs: List[Dict[str, Any]]) -> None:
    """Сохраняем список документов обратно в ingest_manifest.yaml."""
    data = {"documents": docs}
    with MANIFEST_PATH.open("w", encoding="utf-8") as f:
        yaml.safe_dump(
            data,
            f,
            allow_unicode=True,
            sort_keys=False,
            default_flow_style=False,
        )


def collect_file_groups(raw_root: Path) -> Dict[str, Dict[str, Path]]:
    """
    Рекурсивно обходим data/raw, собираем файлы с допустимыми расширениями
    и группируем по "stem-ключу".

    stem_key = относительный путь от RAW_ROOT без расширения.
    Например:
      data/raw/sub/12345.docx  -> stem_key="sub/12345"
      data/raw/12345.pdf       -> stem_key="12345"

    Это значит:
      - файлы в разных подпапках с одинаковым именем считаются разными группами;
      - внутри одной группы (.docx / .pptx / .pdf) выбираем по приоритету.
    """
    groups: Dict[str, Dict[str, Path]] = {}

    for path in raw_root.rglob("*"):
        if not path.is_file():
            continue

        suffix = path.suffix.lower()
        if suffix not in ALLOWED_SUFFIXES:
            continue

        # относительный путь от RAW_ROOT
        rel = path.relative_to(raw_root)  # например "sub/Test-RAG.docx"
        # ключ группы — относительный путь БЕЗ расширения
        stem_key = str(rel.with_suffix(""))  # "sub/Test-RAG" или "Test-RAG"

        if stem_key not in groups:
            groups[stem_key] = {}
        # в рамках stem_key храним файлы по расширению
        groups[stem_key][suffix] = path

    return groups


def pick_best_files(groups: Dict[str, Dict[str, Path]]) -> Dict[str, Path]:
    """
    Для каждой группы выбираем лучший файл по приоритету расширений.
    Возвращаем dict: stem_key -> Path (выбранный файл).
    """
    result: Dict[str, Path] = {}

    for stem_key, variants in groups.items():
        chosen: Path | None = None
        for ext in PREFERRED_SUFFIX_ORDER:
            if ext in variants:
                chosen = variants[ext]
                break
        if chosen is None:
            # теоретически не должно случиться (фильтрация по ALLOWED_SUFFIXES выше),
            # но на всякий случай пропускаем.
            continue
        result[stem_key] = chosen

    return result


def sync_with_manifest(
    chosen_files: Dict[str, Path],
    manifest_docs: List[Dict[str, Any]],
    dry_run: bool,
) -> Tuple[int, int, int]:
    """
    Синхронизация скана диска с манифестом.

    Возвращает:
      total_groups   – сколько групп файлов (stem) найдено на диске
      already_known  – сколько из выбранных файлов уже есть в манифесте (по полю file)
      new_count      – сколько новых записей будет добавлено
      archived_count – сколько записей будет помечено archived (файл исчез с диска)
    """
    total_groups = len(chosen_files)

    # Множество путей файлов, которые есть на диске (с учётом выбора по расширению)
    disk_file_paths = {str(path) for path in chosen_files.values()}

    # Множество путей файлов, которые уже в манифесте
    existing_files = {doc.get("file") for doc in manifest_docs if doc.get("file")}

    # 1) считаем, сколько из выбранных файлов уже отражены в манифесте
    already_known = sum(1 for f in disk_file_paths if f in existing_files)

    # 2) добавляем новые записи (те, которых нет в манифесте по полю file)
    new_docs: List[Dict[str, Any]] = []
    for stem_key, path in chosen_files.items():
        file_str = str(path)  # например: "data/raw/sub/Test-RAG.docx"
        if file_str in existing_files:
            continue

        # Базовый id — из stem_key, заменяем слеши на дефисы
        base_id = stem_key.replace("/", "-").replace("\\", "-")
        new_doc = {
            "id": base_id,
            "file": file_str,
            "slug": base_id,      # можно потом руками переопределить
            "title": None,
            "subtitle": None,
            "series": None,
            "version": 1,
            "language": "ru",
            "doc_type": "book",   # по умолчанию; можно менять в YAML вручную
            "status": "active",
            "tags": [],
        }
        new_docs.append(new_doc)

    new_count = len(new_docs)

    # 3) помечаем как archived те записи манифеста, у которых файл исчез с диска
    archived_count = 0
    for doc in manifest_docs:
        f = doc.get("file")
        if not f:
            continue
        if f not in disk_file_paths and doc.get("status") != "archived":
            doc["status"] = "archived"
            archived_count += 1

    # Если не dry_run — физически дописываем новые документы и сохраняем YAML
    if not dry_run:
        if new_docs or archived_count:
            manifest_docs.extend(new_docs)
            save_manifest(manifest_docs)

    return total_groups, already_known, new_count, archived_count


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Сканирует data/raw (включая подпапки), выбирает приоритетные файлы "
                    "по расширениям и синхронизирует их с ingest_manifest.yaml."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Только показать, что будет сделано, без изменения ingest_manifest.yaml",
    )
    args = parser.parse_args()

    print(f"[SCAN] RAW root: {RAW_ROOT}")

    manifest_docs = load_manifest()

    groups = collect_file_groups(RAW_ROOT)
    chosen_files = pick_best_files(groups)

    total_groups, already_known, new_count, archived_count = sync_with_manifest(
        chosen_files=chosen_files,
        manifest_docs=manifest_docs,
        dry_run=args.dry_run,
    )

    print(f"[SCAN] Найдено групп файлов (по stem): {total_groups}")
    print(f"[SCAN] Уже были в манифесте (по file): {already_known}")
    print(f"[SCAN] Новых записей для добавления: {new_count}")
    print(f"[SCAN] Записей помечено как archived (файла нет на диске): {archived_count}")

    if args.dry_run:
        print("[SCAN] Режим dry-run: манифест НЕ будет изменён.")
    else:
        print("[SCAN] Манифест ОБНОВЛЁН.")


if __name__ == "__main__":
    main()
