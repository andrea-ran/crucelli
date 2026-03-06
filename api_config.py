import os


def _load_dotenv(path):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
_load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

API_KEY = os.getenv("API_FOOTBALL_KEY", "").strip()
HEADERS = {"x-apisports-key": API_KEY} if API_KEY else {}


def require_api_key():
    if not API_KEY:
        raise RuntimeError(
            "API_FOOTBALL_KEY non impostata. "
            "Esempio: export API_FOOTBALL_KEY=la_tua_chiave"
        )
    return API_KEY
