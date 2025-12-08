import argparse
from typing import Optional, List

from openai import OpenAI

from rag.retrieval import retrieve_top_k, Chunk, get_openai_client


SYSTEM_PROMPT = """
Ты — ассистент, отвечающий на вопросы строго по предоставленному контексту.
1. Используй только факты из контекста.
2. Если ответ в контексте не найден, честно напиши, что в предоставленных фрагментах нет ответа.
3. Не придумывай новых фактов и не достраивай контент книги.
4. Отвечай на русском языке, структурировано и по делу.
"""


def build_prompt(query: str, chunks: List[Chunk]) -> str:
    """
    Формируем текстовый промпт вида:
    [CONTEXT]
    ===
    [QUESTION]
    """
    context_parts = []
    for i, ch in enumerate(chunks, start=1):
        context_parts.append(f"Фрагмент {i}:\n{ch.text}\n")

    context_text = "\n\n".join(context_parts)

    prompt = f"""
Контекст:
{context_text}

===

Вопрос:
{query}
""".strip()

    return prompt


def ask_rag(query: str, slug: Optional[str], k: int = 5, preload_limit: Optional[int] = None) -> str:
    """
    Полный цикл:
    - поиск релевантных чанков
    - генерация ответа модели
    """
    # 1. Поиск чанков
    scored = retrieve_top_k(query=query, slug=slug, k=k, preload_limit=preload_limit)
    chunks = [ch for ch, _sim in scored]

    if not chunks:
        return "В базе нет подходящих фрагментов (чанков) для ответа на этот вопрос."

    # 2. Построение промпта
    prompt = build_prompt(query, chunks)

    # 3. Вызов модели
    client: OpenAI = get_openai_client()
    resp = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        temperature=0.1,
    )

    return resp.choices[0].message.content.strip()


def main():
    parser = argparse.ArgumentParser(description="Простой CLI для RAG по книгам.")
    parser.add_argument("--query", "-q", required=True, help="Вопрос к базе знаний.")
    parser.add_argument(
        "--slug",
        "-s",
        required=False,
        help="Опционально: slug документа (книги), например 'plemennoy-marketing'. "
             "Если не указан — поиск по всему корпусу.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=5,
        help="Количество чанков для ответа (по умолчанию 5).",
    )
    parser.add_argument(
        "--preload-limit",
        type=int,
        default=None,
        help="Опционально: ограничить количество загружаемых чанков "
             "для ускорения (по умолчанию все).",
    )

    args = parser.parse_args()

    answer = ask_rag(
        query=args.query,
        slug=args.slug,
        k=args.top_k,
        preload_limit=args.preload_limit,
    )
    print("\n=== ОТВЕТ ===\n")
    print(answer)


if __name__ == "__main__":
    main()
