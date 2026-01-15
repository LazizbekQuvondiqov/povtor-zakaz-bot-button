# config.py
import os
from dotenv import load_dotenv

# .env faylini yuklash
load_dotenv()
SUPER_ADMIN_ID = 1205534758
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
BILLZ_SECRET_KEY = os.getenv("BILLZ_SECRET_KEY")
ALL_SHOPS_IDS = os.getenv("BILLZ_SHOP_IDS")
POSTGRES_URL = os.getenv("POSTGRES_DATABASE_URL")

# Admin ID'larni string'dan integer list'ga o'tkazish
try:
    ADMIN_IDS = [int(admin_id.strip()) for admin_id in os.getenv("ADMIN_IDS", "").split(',')]
except (ValueError, AttributeError):
    print("DIQQAT: .env faylidagi ADMIN_IDS noto'g'ri formatda yoki mavjud emas!")
    ADMIN_IDS = []

# Ma'lumotlar saqlanadigan SQLite bazasi manzili
DB_PATH = "database/Data_Model.db"
PRODUCTS_JSON_FILE = "products_cataloglar.json"
LAST_SYNC_FILE = "database/catalog_last_sync.txt"

# Tekshiruv
if not all([TELEGRAM_BOT_TOKEN, BILLZ_SECRET_KEY, ALL_SHOPS_IDS, POSTGRES_URL]):
    raise ValueError("DIQQAT: .env faylidagi barcha kerakli o'zgaruvchilarni to'ldiring!")
# config.py faylining oxiriga qo'shing:
ARCHIVE_CHANNEL_ID = -1003365677889  # <-- BU YERGA KANAL ID SINI YOZING (boshida -100 bo'lishi shart)
