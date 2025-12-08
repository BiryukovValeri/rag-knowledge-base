import os
from openai import OpenAI

client = OpenAI(api_key=os.getenv("EMBEDDINGS_API_KEY"))

def embed_text(text: str) -> list[float]:
    """
    Делает embedding через OpenAI.
    Возвращает массив чисел (vector).
    """
    resp = client.embeddings.create(
        model="text-embedding-3-large",
        input=text
    )
    return resp.data[0].embedding