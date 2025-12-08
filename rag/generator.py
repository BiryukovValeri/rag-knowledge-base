import os
from openai import OpenAI

# Используем отдельный ключ для LLM (может совпадать с OPENAI_API_KEY)
LLM_API_KEY = os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY")

if not LLM_API_KEY:
    raise RuntimeError("LLM_API_KEY / OPENAI_API_KEY не задан в переменных окружения")

client = OpenAI(api_key=LLM_API_KEY)


def generate_answer(question: str, context: str) -> str:
    """
    Генерация финального ответа через OpenAI Chat Completions.

    question: вопрос пользователя
    context: текстовые фрагменты (RAG-контекст), уже собранные из Supabase
    """
    messages = [
        {
            "role": "system",
            "content": (
                "Ты деловой консультант и автор книг. "
                "Отвечай структурировано, без воды, опираясь на переданный контекст. "
                "Если в контексте нет ответа — честно скажи об этом."
            ),
        },
        {
            "role": "user",
            "content": f"Вопрос:\n{question}\n\nКонтекст:\n{context}",
        },
    ]

    resp = client.chat.completions.create(
        model="gpt-4.1-mini",  # можно заменить на gpt-4.1 / gpt-4.1-preview при желании
        messages=messages,
        temperature=0.2,
    )

    # В openai>=2.0 правильный доступ так:
    return resp.choices[0].message.content
