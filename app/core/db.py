from loguru import logger
from supabase import create_client, Client

from app.core.config import SUPABASE_URL, SUPABASE_SERVICE_KEY


def get_supabase_client() -> Client:
    """
    Клиент Supabase для серверной логики (ingest, RAG API).
    Использует service key, чтобы иметь полный доступ к БД.
    """
    logger.info("Creating Supabase client (service role)...")
    if SUPABASE_URL is None or SUPABASE_SERVICE_KEY is None:
        raise RuntimeError("SUPABASE_URL or SUPABASE_SERVICE_KEY is not set")
    client: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    return client


def test_connection() -> None:
    """
    Простейшая проверка: запрос к таблице documents с ограничением 1.
    """
    try:
        client = get_supabase_client()
        logger.info("Running test query to 'documents' table via Supabase HTTP API...")
        response = client.table("documents").select("*").limit(1).execute()
        logger.info(f"Supabase HTTP test response: {response}")
        logger.info("Supabase HTTP connection test: OK")
    except Exception as e:
        logger.error(f"Supabase HTTP connection test FAILED: {e}")
        raise


if __name__ == "__main__":
    test_connection()
