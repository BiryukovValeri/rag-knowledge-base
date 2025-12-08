import os
from dotenv import load_dotenv

load_dotenv()


def get_env_var(name: str, required: bool = True, default: str | None = None) -> str | None:
    value = os.getenv(name, default)
    if required and (value is None or value == ""):
        raise RuntimeError(f"Environment variable {name} is required but not set")
    return value


# ======================
# Supabase
# ======================

SUPABASE_URL: str = get_env_var("SUPABASE_URL", required=True)
SUPABASE_ANON_KEY: str | None = get_env_var("SUPABASE_ANON_KEY", required=False, default=None)
SUPABASE_SERVICE_KEY: str = get_env_var("SUPABASE_SERVICE_KEY", required=True)

# Это алиас для обратной совместимости (некоторые модули импортируют SUPABASE_KEY)
SUPABASE_KEY: str = SUPABASE_SERVICE_KEY


# ======================
# Optional Postgres URL
# ======================

DATABASE_URL: str | None = get_env_var("DATABASE_URL", required=False, default=None)


# ======================
# OpenAI API keys
# ======================

# Старое имя — OPENAI_API_KEY
OPENAI_API_KEY: str | None = get_env_var("OPENAI_API_KEY", required=False, default=None)

# Если EMBEDDINGS_API_KEY не задан — используем OPENAI_API_KEY
EMBEDDINGS_API_KEY: str | None = os.getenv("EMBEDDINGS_API_KEY") or OPENAI_API_KEY

# Если LLM_API_KEY не задан — используем OPENAI_API_KEY
LLM_API_KEY: str | None = os.getenv("LLM_API_KEY") or OPENAI_API_KEY
