import os
import httpx
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

if not TOKEN:
    print("ОШИБКА: переменная TELEGRAM_BOT_TOKEN не найдена в .env")
    raise SystemExit(1)

URL = f"https://api.telegram.org/bot{TOKEN}/getMe"

def main():
    print("Пробую вызвать Bot API:", URL.replace(TOKEN, "<HIDDEN>"))
    try:
        r = httpx.get(URL, timeout=10)
        print("STATUS:", r.status_code)
        print("TEXT:", r.text[:500])
    except Exception as e:
        print("ERROR:", repr(e))

if __name__ == "__main__":
    main()
