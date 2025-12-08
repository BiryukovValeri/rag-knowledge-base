from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Union, Any

from loguru import logger

from ingest.extract_text.docx_reader import Block, extract_blocks_from_docx, debug_print_blocks


# Универсальный тип блока: либо dataclass Block, либо dict
BlockLike = Union[Block, dict[str, Any]]


def _get_block_text(block: BlockLike) -> str:
    """Безопасно достаёт текст блока для Block и dict."""
    if hasattr(block, "text"):
        return (getattr(block, "text") or "").strip()
    if isinstance(block, dict):
        return (block.get("text") or "").strip()
    return str(block).strip()


def _get_block_level(block: BlockLike) -> Optional[int]:
    """Безопасно достаёт уровень заголовка (level), если он есть."""
    if hasattr(block, "level"):
        return getattr(block, "level")
    if isinstance(block, dict):
        return block.get("level")
    return None


def _get_block_type(block: BlockLike) -> str:
    """Тип блока для отладки (heading/paragraph и т.п.)."""
    if hasattr(block, "type"):
        return str(getattr(block, "type"))
    if isinstance(block, dict):
        return str(block.get("type", "unknown"))
    return type(block).__name__


@dataclass
class Section:
    """
    Логический раздел книги.
    В идеале соответствует заголовку H1, но при отсутствии H1
    умеет представлять весь документ как одну секцию.
    """
    index: int                             # порядковый номер секции (1..N)
    title: str                             # текст заголовка секции
    heading_block: Optional[BlockLike]     # сам блок заголовка (или None, если fallback)
    blocks: List[BlockLike] = field(default_factory=list)  # все блоки внутри секции


def split_into_sections(blocks: List[BlockLike]) -> List[Section]:
    """
    Разбивает последовательность блоков на секции.

    Логика:
    - Если есть блоки с level == 1 → каждая H1 открывает новую секцию.
    - Все блоки между H1 относятся к соответствующей секции.
    - Если ни одного H1 нет → создаём одну секцию из всего документа
      (fallback для неструктурированных файлов).
    """
    sections: List[Section] = []
    has_h1 = False

    current_blocks: List[BlockLike] = []
    current_title: Optional[str] = None
    current_heading_block: Optional[BlockLike] = None
    current_index = 0

    for block in blocks:
        level = _get_block_level(block)

        if level == 1:
            has_h1 = True

            # закрываем предыдущую секцию, если она была
            if current_blocks:
                sections.append(
                    Section(
                        index=current_index,
                        title=current_title or "",
                        heading_block=current_heading_block,
                        blocks=current_blocks,
                    )
                )

            # открываем новую секцию
            current_index += 1
            current_heading_block = block
            title_text = _get_block_text(block)
            current_title = title_text or f"Section {current_index}"
            current_blocks = [block]
        else:
            # просто накапливаем контент
            current_blocks.append(block)

    # добиваем хвост
    if current_blocks:
        if has_h1:
            # нормальный случай: последняя секция с H1
            sections.append(
                Section(
                    index=current_index,
                    title=current_title or "",
                    heading_block=current_heading_block,
                    blocks=current_blocks,
                )
            )
        else:
            # fallback: ни одного H1, весь документ — одна секция
            first_text = _get_block_text(blocks[0]) if blocks else ""
            sections = [
                Section(
                    index=1,
                    title=first_text or "FULL_DOCUMENT",
                    heading_block=None,
                    blocks=blocks,
                )
            ]

    logger.info(
        "Split into %d sections (H1-based if present, fallback to single FULL_DOCUMENT)",
        len(sections),
    )
    return sections


def debug_print_sections(sections: List[Section], limit: int = 20) -> None:
    """
    Печатает краткий обзор первых N секций:
    - номер
    - заголовок
    - количество блоков внутри
    - первые 1–2 блока секции (тип + укороченный текст)
    """
    logger.info(f"Printing first {min(limit, len(sections))} sections:")
    for s in sections[:limit]:
        print("=" * 80)
        print(f"SECTION {s.index}: {s.title}")
        print(f"Blocks inside: {len(s.blocks)}")
        if s.blocks:
            b0 = s.blocks[0]
            preview = _get_block_text(b0)
            if len(preview) > 100:
                preview = preview[:97] + "..."
            print(f"  First block: [{_get_block_type(b0)}] {preview}")
        if len(s.blocks) > 1:
            b1 = s.blocks[1]
            preview2 = _get_block_text(b1)
            if len(preview2) > 100:
                preview2 = preview2[:97] + "..."
            print(f"  Second block: [{_get_block_type(b1)}] {preview2}")


def build_text_chunks_for_section(
    section: Section,
    max_chars: int = 1200,
    min_chars: int = 600,
) -> List[str]:
    """
    Формирует текстовые чанки внутри секции.

    Логика:
    - собираем текст блоков (H2/H3 и абзацы) в буфер с разделителем "\n\n";
    - как только длина буфера > max_chars — фиксируем chunk и начинаем новый;
    - хвост < min_chars, если есть предыдущий chunk — приклеиваем к нему.
    В каждый chunk в начало добавляем заголовок секции (H1), чтобы не потерять контекст.

    Работает и с Block, и с dict-блоками.
    """
    pieces: List[str] = []

    # Заголовок секции как первый контекстный блок
    header = section.title.strip()

    for b in section.blocks:
        txt = _get_block_text(b)
        if not txt:
            continue
        pieces.append(txt)

    if not pieces:
        # Пустая секция — либо пропускаем, либо один короткий chunk только с заголовком
        return [header] if header else []

    chunks: List[str] = []
    current: List[str] = []
    current_len = 0

    def flush_current() -> None:
        nonlocal current, current_len
        if not current:
            return
        body = "\n\n".join(current)
        if header:
            full_text = header + "\n\n" + body
        else:
            full_text = body
        chunks.append(full_text)
        current = []
        current_len = 0

    for piece in pieces:
        # +2 за "\n\n", если в current уже есть текст
        extra_len = len(piece) + (2 if current else 0)
        if current_len + extra_len > max_chars and current:
            flush_current()
        current.append(piece)
        current_len += extra_len

    # Хвост
    if current:
        tail_text = "\n\n".join(current)
        if chunks and len(tail_text) < min_chars:
            # маленький хвост — приклеиваем к предыдущему чанку
            chunks[-1] = chunks[-1] + "\n\n" + tail_text
        else:
            flush_current()

    return chunks


if __name__ == "__main__":
    # Локальный тест: разбиение конкретной книги на секции и просмотр
    docx_path = Path("data/raw/Книга 1 Стратегический интеллект Стратегические инструменты.docx")
    logger.info(f"Loading blocks from: {docx_path}")
    blocks = extract_blocks_from_docx(docx_path)
    logger.info(f"Got {len(blocks)} blocks. Example of first 10:")
    debug_print_blocks(blocks, limit=10)

    sections = split_into_sections(blocks)
    debug_print_sections(sections, limit=20)
