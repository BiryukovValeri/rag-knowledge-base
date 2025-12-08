import httpx

URL = "https://api.telegram.org"

def main():
    print("Пробую подключиться к", URL)
    try:
        r = httpx.get(URL, timeout=10)
        print("STATUS:", r.status_code)
        print("HEADERS server:", r.headers.get("server"))
        print("TEXT (first 200):", r.text[:200])
    except Exception as e:
        print("ERROR:", repr(e))

if __name__ == "__main__":
    main()
