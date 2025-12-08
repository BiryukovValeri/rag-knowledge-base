from __future__ import annotations

from pathlib import Path
from typing import Literal, TypedDict, List

import re
from docx import Document
from loguru import logger


BlockType = Literal["heading", "paragraph"]


class Block(TypedDict, total=False):
    type: BlockType
    level: int | None
    text: str


def detect_heading_level(style_name: str | None) -> int | None:
    """
    Определяет уровень заголовка по имени стиля Word.
    Поддерживает как русские, так и английские стили:
    - Heading 1 / Heading 2 / ...
    - Заголовок 1 / Заголовок 2 / ...
    """
    if not style_name:
        return None

    name = style_name.strip().lower()

    # Варианты "heading 1", "заголовок 1"
    m = re.match(r"(heading|заголовок)\s*(\d+)", name)
    if m:
        try:
            return int(m.group(2))
        except ValueError:
            return None

    # Иногда стили называются просто "Heading1" / "Заголовок1"
    m2 = re.match(r"(heading|заголовок)(\d+)", name)
    if m2:
        try:
            return int(m2.group(2))
        except ValueError:
            return None

    return None


def extract_blocks_from_docx(path: str | Path) -> List[Block]:
    """
    Читает DOCX и возвращает список блоков:
    - заголовки (type='heading', level=1/2/3...)
    - обычные абзацы (type='paragraph')
    Пустые строки выбрасываются.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"DOCX file not found: {path}")

    logger.info(f"Loading DOCX: {path}")
    doc = Document(path)

    blocks: List[Block] = []

    for p in doc.paragraphs:
        text = p.text.strip()
        if not text:
            continue  # пропускаем пустые абзацы

        style_name = None
        try:
            if p.style is not None and p.style.name:
                style_name = str(p.style.name)
        except Exception:
            # иногда у стиля бывают странные состояния — просто игнорируем
            style_name = None

        level = detect_heading_level(style_name)

        if level is not None:
            blocks.append(
                Block(
                    type="heading",
                    level=level,
                    text=text,
                )
            )
        else:
            blocks.append(
                Block(
                    type="paragraph",
                    level=None,
                    text=text,
                )
            )

    logger.info(f"Extracted {len(blocks)} blocks from DOCX")
    return blocks


def debug_print_blocks(blocks: List[Block], limit: int = 40) -> None:
    """
    Печатает первые N блоков для визуальной проверки структуры книги.
    """
    logger.info(f"Printing first {min(limit, len(blocks))} blocks:")
    for i, b in enumerate(blocks[:limit], start=1):
        if b["type"] == "heading":
            lvl = b.get("level") or 0
            print(f"{i:03d} [H{lvl}] {b['text']}")
        else:
            txt = b["text"]
            if len(txt) > 80:
                txt = txt[:77] + "..."
            print(f"{i:03d} [P ] {txt}")


if __name__ == "__main__":
    # Путь к твоей книге (если имя файла изменишь — поменяй и здесь)
    docx_path = Path("data/raw/Книга 1 Стратегический интеллект Стратегические инструменты.docx")
    blocks = extract_blocks_from_docx(docx_path)
    debug_print_blocks(blocks, limit=50)
