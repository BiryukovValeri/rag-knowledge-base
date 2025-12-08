from __future__ import annotations

from typing import List, Optional, Literal, Dict

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from rag.retrieval import retrieve_top_k, get_openai_client
from app.core.db import get_supabase_client

app = FastAPI()
templates = Jinja2Templates(directory="app/templates")

@app.get("/debug/env")
def debug_env():
    import os
    from fastapi.responses import JSONResponse

    keys = [
        "SUPABASE_URL",
        "SUPABASE_KEY",
        "SUPABASE_SERVICE_KEY",
        "SUPABASE_ANON_KEY",
        "DATABASE_URL",
        "OPENAI_API_KEY",
        "LLM_API_KEY",
        "EMBEDDINGS_API_KEY",
        "TELEGRAM_BOT_TOKEN",
        "RAG_URL",
    ]

    info = {}
    for k in keys:
        v = os.getenv(k)
        info[k] = {
            "present": bool(v),
            "length": len(v) if v else 0,
        }

    return JSONResponse(info)


@app.get("/health")
def health():
    return {"status": "ok"}


# === Модели запросов ===

class RAGRequest(BaseModel):
    query: str
    slug: str | None = None           # для совместимости, можно не использовать
    slugs: list[str] | None = None    # мультивыбор книг
    top_k: int = 5
    preload_limit: int = 2000
    include_meta: bool = False
    mode: Literal["synthesis", "extract"] = "synthesis"


class RAGAnswerRequest(BaseModel):
    query: str
    slug: str | None = None
    slugs: list[str] | None = None
    top_k: int = 5
    preload_limit: int = 2000
    include_meta: bool = True
    mode: Literal["synthesis", "extract"] = "synthesis"


# === НИЗКИЙ УРОВЕНЬ: /rag/query — «сырые» чанки ===

@app.post("/rag/query")
def rag_query(body: RAGRequest):
    scored = retrieve_top_k(
        query=body.query,
        slug=body.slug,
        slugs=body.slugs,
        k=body.top_k,
        preload_limit=body.preload_limit,
    )

    base_results = [
        {
            "chunk_id": ch.id,
            "document_id": ch.document_id,
            "score": score,
            "text": ch.text[:500],
        }
        for ch, score in scored
    ]

    if not body.include_meta:
        return {
            "count": len(base_results),
            "results": base_results,
        }

    supabase = get_supabase_client()
    doc_ids = list({row["document_id"] for row in base_results})

    if doc_ids:
        resp = (
            supabase.table("documents")
            .select("id, title, series")
            .in_("id", doc_ids)
            .execute()
        )
        doc_rows = resp.data or []
        doc_map = {row["id"]: row for row in doc_rows}
    else:
        doc_map = {}

    for row in base_results:
        doc = doc_map.get(row["document_id"])
        if doc:
            row["book_title"] = doc.get("title")
            row["book_series"] = doc.get("series")
        else:
            row["book_title"] = None
            row["book_series"] = None

        row["author"] = "Валерий Бирюков"

    return {
        "count": len(base_results),
        "results": base_results,
    }


# === ВЫСОКИЙ УРОВЕНЬ: /rag/answer — готовый ответ + цитаты ===

@app.post("/rag/answer")
def rag_answer(body: RAGAnswerRequest):
    scored = retrieve_top_k(
        query=body.query,
        slug=body.slug,
        slugs=body.slugs,
        k=body.top_k,
        preload_limit=body.preload_limit,
    )

    if not scored:
        return {
            "answer": "В базе не найдено ни одного фрагмента, связанного с запросом.",
            "citations": [],
        }

    chunks = [ch for ch, _score in scored]
    doc_ids = list({ch.document_id for ch in chunks})

    supabase = get_supabase_client()
    doc_meta_resp = (
        supabase.table("documents")
        .select("id, title, series")
        .in_("id", doc_ids)
        .execute()
    )
    meta_rows = doc_meta_resp.data or []
    meta_by_id = {row["id"]: row for row in meta_rows}

    context_parts: List[str] = []
    citations = []

    for idx, (ch, score) in enumerate(scored, start=1):
        meta = meta_by_id.get(ch.document_id, {})
        book_title = meta.get("title")
        book_series = meta.get("series")

        context_parts.append(
            f"Источник {idx}.\n"
            f"Книга: {book_title or 'неизвестно'}\n"
            f"Серия: {book_series or 'неизвестно'}\n\n"
            f"Текст фрагмента:\n{ch.text}"
        )

        citations.append(
            {
                "index": idx,
                "chunk_id": ch.id,
                "document_id": ch.document_id,
                "score": score,
                "book_title": book_title,
                "book_series": book_series,
                "author": "Валерий Бирюков",
            }
        )

    context_str = "\n\n---\n\n".join(context_parts)

    mode = body.mode or "synthesis"

    if mode == "extract":
        system_prompt = (
            "Ты отвечаешь ТОЛЬКО на основе переданных фрагментов книг. "
            "Твоя задача — максимально буквальный, аккуратный ответ. "
            "Избегай свободных интерпретаций и обобщений, не придумывай того, "
            "чего нет в текстах. Если данных недостаточно, прямо укажи это. "
            "Отвечай по-русски, кратко и по существу."
        )
        user_instruction = (
            "Сформулируй краткий ответ (3–6 предложений), опираясь на дословные формулировки "
            "из фрагментов. При необходимости цитируй ключевые фразы. "
            "Не добавляй собственных гипотез. "
            "В конце добавь блок «Источники» с перечислением использованных Источников (1, 2, …) "
            "без пересказа их содержания."
        )
        temperature = 0.1
    else:
        system_prompt = (
            "Ты отвечаешь ТОЛЬКО на основе переданных фрагментов книг. "
            "Твоя задача — синтезировать и обобщить идеи из фрагментов, "
            "но не придумывать факты, которых там нет. "
            "Можно перефразировать и связывать мысли, но любые выводы должны "
            "логически следовать из текстов. Если данных недостаточно, прямо скажи об этом. "
            "Отвечай по-русски, структурированно и без лишней воды."
        )
        user_instruction = (
            "Сформулируй связный обобщённый ответ на вопрос, аккуратно объединяя идеи из фрагментов. "
            "Поясни ключевые смыслы и взаимосвязи, но не выходи за рамки того, что явно или неявно "
            "следует из текстов. "
            "В конце добавь блок «Источники» с кратким перечислением книг по номерам Источников (1, 2, …)."
        )
        temperature = 0.2

    client = get_openai_client()
    completion = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {
                "role": "system",
                "content": system_prompt,
            },
            {
                "role": "user",
                "content": (
                    f"Вопрос пользователя:\n{body.query}\n\n"
                    f"Ниже фрагменты из книг (с источниками):\n\n{context_str}\n\n"
                    f"{user_instruction}"
                ),
            },
        ],
        temperature=temperature,
    )

    answer_text = completion.choices[0].message.content

    return {
        "answer": answer_text,
        "citations": citations if body.include_meta else [],
    }


# === ХЕЛПЕР: построение групп книг для UI ===

SERIES_ORDER: List[str] = [
    "Интеллекты",
    "Формула Управления. Точка расчёта",
    "IntlX",
    "misc",
]

# Явный порядок заголовков внутри серий
TITLE_ORDER: Dict[str, List[str]] = {
    "Интеллекты": [
        "Книга 1. Стратегический интеллект. Стратегические инструменты.",
        "Книга 2. Операционный интеллект. Тактические и оперативные инструменты.",
        "Книга 3. Искусственный интеллект. Синтез мышления и действий через ИИ.",
        "Книга 4. Организационный интеллект. Архитектура устойчивости.",
        "Книга 5. Лидерский интеллект. Как изменить компанию — и не сгореть.",
        "Книга 6. Финансовый интеллект. Оборона ликвидности и превращение в преимущество.",
        "Книга 7. Клиентский интеллект. Механизмы удержания и вдохновения.",
    ],
    "Формула Управления. Точка расчёта": [
        "Формула Управления. Точка расчёта. Том 1. Маркетплейсы.",
        "Формула Управления. Точка расчёта. Том 2. Финансы и P&L.",
        "Формула Управления. Точка расчёта. Том 3. Продажи и ассортимент.",
        "Формула Управления. Точка расчёта. Том 4. Производство и логистика.",
    ],
    "IntlX": [
        "Пятикратно точный: Методика оценки руководителей",
        "Тверже стали, тоньше льда: переговоры как система превосходства",
        "AI-first. Трансформация без иллюзий",
        "Компания-Оркестр. Директора как инструмент синхронизации людей, данных, смысла",
        "Омниканальные продажи: от боли к прибыли",
        "Шоки-2026: 48 часов / 30 дней / Food / Non Food",
        "Племенной маркетинг",
        # рабочие тетради — пойдут после основных
    ],
}


def build_book_groups() -> List[Dict]:
    supabase = get_supabase_client()
    resp = (
        supabase.table("documents")
        .select("slug, title, series")
        .eq("status", "active")
        .execute()
    )
    docs = resp.data or []

    # группировка по series
    by_series: Dict[str, List[Dict]] = {}
    for row in docs:
        series = row.get("series") or "misc"
        by_series.setdefault(series, []).append(row)

    # порядок серий
    def series_key(s: str) -> int:
        if s in SERIES_ORDER:
            return SERIES_ORDER.index(s)
        return len(SERIES_ORDER) + 1

    book_groups: List[Dict] = []
    for series in sorted(by_series.keys(), key=series_key):
        books = by_series[series]
        order_list = TITLE_ORDER.get(series, [])

        def title_key(b: Dict) -> int:
            title = b.get("title") or ""
            if title in order_list:
                return order_list.index(title)
            return len(order_list) + 1

        books_sorted = sorted(books, key=title_key)

        book_groups.append(
            {
                "series_label": series,
                "books": [
                    {
                        "slug": b["slug"],
                        "title": b["title"],
                    }
                    for b in books_sorted
                ],
            }
        )

    return book_groups


# === ПРОСТОЙ ВЕБ-ИНТЕРФЕЙС ===

@app.get("/", response_class=HTMLResponse)
async def index_get(request: Request):
    book_groups = build_book_groups()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "query": "",
            "top_k": 5,
            "preload_limit": 2000,
            "include_meta": True,
            "mode": "synthesis",
            "answer": None,
            "citations": [],
            "book_groups": book_groups,
            "selected_slugs": [],
        },
    )


@app.post("/", response_class=HTMLResponse)
async def index_post(
    request: Request,
    query: str = Form(...),
    top_k: int = Form(5),
    preload_limit: int = Form(2000),
    slugs: Optional[List[str]] = Form(None),
    include_meta: Optional[str] = Form(None),
    mode: str = Form("synthesis"),
):
    include_meta_bool = include_meta is not None
    selected_slugs = slugs or []

    body = RAGAnswerRequest(
        query=query,
        slug=None,
        slugs=selected_slugs if selected_slugs else None,
        top_k=top_k,
        preload_limit=preload_limit,
        include_meta=include_meta_bool,
        mode=mode or "synthesis",
    )

    resp = rag_answer(body)
    book_groups = build_book_groups()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "query": query,
            "top_k": top_k,
            "preload_limit": preload_limit,
            "include_meta": include_meta_bool,
            "mode": mode or "synthesis",
            "answer": resp.get("answer"),
            "citations": resp.get("citations", []),
            "book_groups": book_groups,
            "selected_slugs": selected_slugs,
        },
    )