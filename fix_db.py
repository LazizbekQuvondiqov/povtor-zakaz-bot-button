import asyncio
from db_manager import engine, GeneratedOrder, Base
from sqlalchemy import text

# Sinxron kod bo'lgani uchun to'g'ridan-to'g'ri ishlatamiz
print("⏳ Jadval yangilanmoqda...")

try:
    # 1. Eski jadvalni majburan o'chirish
    GeneratedOrder.__table__.drop(engine)
    print("✅ Eski 'generated_orders' jadvali o'chirildi.")
except Exception as e:
    print(f"⚠️ O'chirishda xatolik (balki jadval yo'qdir): {e}")

try:
    # 2. Yangi jadvalni yaratish (Yangi ustunlar bilan)
    GeneratedOrder.__table__.create(engine)
    print("✅ Yangi 'generated_orders' jadvali muvaffaqiyatli yaratildi.")
    print("👉 Endi botdan /force_update buyrug'ini yuboring.")
except Exception as e:
    print(f"❌ Yaratishda xatolik: {e}")
