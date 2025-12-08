import os

from dotenv import load_dotenv

load_dotenv()


def get_env_var(name: str, required: bool = True, default: str | None = None) -> str | None:
    value = os.getenv(name, default)
    if required and (value is None or value == ""):
        raise RuntimeError(f"Environment variable {name} is required but not set")
    return value


# --- Supabase HTTP API ---
SUPABASE_URL: str | None = get_env_var("SUPABASE_URL", required=True)
SUPABASE_ANON_KEY: str | None = get_env_var("SUPABASE_ANON_KEY", required=False, default=None)
SUPABASE_SERVICE_KEY: str | None = get_env_var("SUPABASE_SERVICE_KEY", required=True)

# --- (опциональный старый DATABASE_URL) ---
DATABASE_URL: str | None = get_env_var("DATABASE_URL", required=False, default=None)

# --- Ключи для LLM (будут нужны позже) ---
EMBEDDINGS_API_KEY: str | None = get_env_var("EMBEDDINGS_API_KEY", required=False, default=None)
LLM_API_KEY: str | None = get_env_var("LLM_API_KEY", required=False, default=None)
