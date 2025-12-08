from loguru import logger
from supabase import Client

from app.core.db import get_supabase_client


def insert_test_document(client: Client) -> None:
    """
    Вставляет один тестовый документ и один тестовый chunk в базу.
    Это проверка, что запись в Supabase работает.
    """
    logger.info("Inserting test document into 'documents' table...")

    # 1. Создаём документ
    doc_data = {
        "slug": "test-document-001",
        "title": "Тестовый документ для проверки пайплайна",
        "subtitle": "Этот документ создан автоматически",
        "series": "Тесты",
        "doc_type": "книга",
        "version": 1,
        "language": "ru",
        "status": "active",
    }

    doc_response = client.table("documents").insert(doc_data).execute()
    logger.info(f"Insert document response: {doc_response}")

    if not doc_response.data:
        raise RuntimeError("No data returned after inserting document")

    document_id = doc_response.data[0]["id"]
    logger.info(f"Created document with id={document_id}")

    # 2. Создаём один тестовый chunk
    chunk_data = {
        "document_id": document_id,
        "section_id": None,
        "chunk_index": 1,
        "page_from": None,
        "page_to": None,
        "text": "Это тестовый фрагмент текста, записанный в таблицу chunks для проверки пайплайна.",
        "embedding": None,
        "tokens_count": None,
        "quality_flag": "ok",
    }

    chunk_response = client.table("chunks").insert(chunk_data).execute()
    logger.info(f"Insert chunk response: {chunk_response}")

    logger.info("Test document and chunk have been inserted successfully.")


if __name__ == "__main__":
    logger.info("Starting test ingestion...")
    supabase_client = get_supabase_client()
    insert_test_document(supabase_client)
    logger.info("Test ingestion finished.")
