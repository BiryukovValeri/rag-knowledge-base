import os
import textwrap
import requests
from dotenv import load_dotenv

from telegram import (
    Update,
    InlineQueryResultArticle,
    InputTextMessageContent,
)
from telegram.ext import (
    ApplicationBuilder,
    MessageHandler,
    CommandHandler,
    InlineQueryHandler,
    ContextTypes,
    filters,
)

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
RAG_URL = os.getenv("RAG_URL", "http://127.0.0.1:8000/rag/answer")

# Простое хранение режима по чату в памяти
CHAT_MODES: dict[int, str] = {}  # chat_id -> mode ("synthesis", "extract", "bullets", "short")

TELEGRAM_MAX_LEN = 4096
SAFE_LIMIT = 3800  # чтобы не попасть в лимит


def _build_payload(query: str, mode: str = "synthesis"):
    return {
        "query": query,
        "slug": None,
        "slugs": None,           # при желании можно добавить фильтрацию по книгам
        "top_k": 5,
        "preload_limit": 2000,
        "include_meta": True,
        "mode": mode,
    }


def _trim_for_telegram(text: str) -> str:
    if len(text) <= SAFE_LIMIT:
        return text
    return text[:SAFE_LIMIT] + "\n\n[Ответ обрезан до лимита Telegram]"


def _format_answer(answer: str, citations: list[dict]) -> str:
    """
    Красивое форматирование ответа:
    - жирный заголовок "Ответ"
    - текст
    - компактный блок источников
    """
    answer = answer.strip() if answer else "Ответ не получен."

    # компактные источники
    lines = []
    for c in citations[:3]:
        title = c.get("book_title") or "Без названия"
        series = c.get("book_series") or "серия не указана"
        lines.append(f"• {title} ({series})")
    sources_block = ""
    if lines:
        sources_block = "\n\n<b>Источники:</b>\n" + "\n".join(lines)

    body = f"<b>Ответ</b>\n\n{answer}{sources_block}"
    return _trim_for_telegram(body)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    CHAT_MODES[chat_id] = "synthesis"

    text = (
        "Это RAG-бот по твоим книгам.\n\n"
        "Просто напиши вопрос — я отвечу, опираясь на базу.\n\n"
        "Команды:\n"
        "/mode — показать текущий режим и подсказки по смене.\n"
        "/help — краткая справка.\n"
    )
    await update.message.reply_text(text)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "Бот работает поверх RAG по твоим книгам.\n\n"
        "Режимы ответа:\n"
        "• synthesis — обобщённый ответ\n"
        "• extract — более буквальный ответ\n"
        "• bullets — ключевые тезисы списком\n"
        "• short — очень короткий ответ (~до 500 символов)\n\n"
        "Сменить режим: /mode synthesis | /mode extract | /mode bullets | /mode short\n"
        "Если режим не задан, используется synthesis."
    )
    await update.message.reply_text(text)


async def mode_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    args = context.args or []

    if not args:
        current = CHAT_MODES.get(chat_id, "synthesis")
        text = (
            f"Текущий режим: <b>{current}</b>\n\n"
            "Доступные режимы:\n"
            "• synthesis — обобщённый ответ\n"
            "• extract — максимально буквальный\n"
            "• bullets — ключевые тезисы\n"
            "• short — очень короткий ответ\n\n"
            "Сменить режим: /mode synthesis | /mode extract | /mode bullets | /mode short"
        )
        await update.message.reply_text(text, parse_mode="HTML")
        return

    new_mode = args[0].strip().lower()
    if new_mode not in {"synthesis", "extract", "bullets", "short"}:
        await update.message.reply_text(
            "Неизвестный режим. Используй: synthesis, extract, bullets, short."
        )
        return

    CHAT_MODES[chat_id] = new_mode
    await update.message.reply_text(f"Режим ответа установлен: {new_mode}")


async def handle_question(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    text = (update.message.text or "").strip()
    if not text:
        return

    chat_id = update.effective_chat.id
    mode = CHAT_MODES.get(chat_id, "synthesis")

    # индикатор "печатает"
    await context.bot.send_chat_action(
        chat_id=chat_id,
        action="typing",
    )

    payload = _build_payload(text, mode=mode)

    try:
        resp = requests.post(RAG_URL, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        await update.message.reply_text(f"Ошибка при обращении к RAG: {e}")
        return

    answer = data.get("answer") or ""
    citations = data.get("citations") or []

    formatted = _format_answer(answer, citations)
    await update.message.reply_text(formatted, parse_mode="HTML")


async def handle_inline_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Inline-режим:
    @bot вопрос
    Возвращаем один короткий ответ (режим 'short').
    """
    query = update.inline_query.query.strip() if update.inline_query.query else ""
    if not query:
        return

    payload = _build_payload(query, mode="short")

    try:
        resp = requests.post(RAG_URL, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        # В inline режиме — лучше молча не отвечать, чем сыпать ошибками
        return

    answer = data.get("answer") or "Ответ не получен."
    citations = data.get("citations") or []

    # компактный текст без жирного заголовка
    text = answer.strip()
    text = _trim_for_telegram(text)

    # заголовок результата — обрезанный вопрос
    title = textwrap.shorten(query, width=64, placeholder="…")

    result = InlineQueryResultArticle(
        id="rag-inline-1",
        title=title,
        description=textwrap.shorten(answer.replace("\n", " "), width=120, placeholder="…"),
        input_message_content=InputTextMessageContent(text),
    )

    await update.inline_query.answer([result], cache_time=0)


def main():
    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не задан в .env")

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("mode", mode_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_question))
    app.add_handler(InlineQueryHandler(handle_inline_query))

    app.run_polling()


if __name__ == "__main__":
    main()