# --- BU DB_MANAGER.PY FAYLINING YANGI VA TO'G'RI KODI (ONLY POSTGRES) ---

import pandas as pd
from datetime import datetime, timedelta, timezone # timezone qo'shildi



from sqlalchemy import create_engine, Column, Integer, String, BigInteger, Boolean, Float, DateTime, Date, text, func
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import IntegrityError
import config

# SQLite kutubxonasini olib tashladik, chunki endi faqat Postgres ishlatamiz.
TASHKENT_TZ = timezone(timedelta(hours=5))
Base = declarative_base()

# --- JADVAL MODELLARI (POSTGRESQL) ---

class InvitedUser(Base):
    __tablename__ = 'invited_users'
    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, unique=True, nullable=False)
    is_registered = Column(Boolean, default=False)

class Supplier(Base):
    __tablename__ = 'suppliers'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    telegram_id = Column(BigInteger, unique=True, nullable=False)

class Admin(Base):
    __tablename__ = 'admins'
    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, unique=True, nullable=False)

class Setting(Base):
    __tablename__ = 'settings'
    id = Column(Integer, primary_key=True)
    rule_name = Column(String, unique=True, nullable=False)
    rule_value = Column(Float, nullable=False)

class GeneratedOrder(Base):
    __tablename__ = 'generated_orders'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Asosiy ma'lumotlar
    zakaz_id = Column(String, index=True)
    supplier = Column(String)
    artikul = Column(String)
    category = Column(String)
    subcategory = Column(String)
    shop = Column(String)
    color = Column(String)
    photo = Column(String)
    supply_price = Column(Float, default=0.0)
    # Zakaz miqdori
    quantity = Column(Integer)

    # --- NOTEBOOKDAGI ANALIZ USTUNLARI ---
    hozirgi_qoldiq = Column(Float)      # Hozirgi_Qoldiq
    prodano = Column(Float)             # Продано
    days_passed = Column(Integer)       # Дней прошло
    ortacha_sotuv = Column(Float)       # o'rtcha sotuv
    kutilyotgan_sotuv = Column(Float)   # kutulyotgan sotuv
    tovar_holati = Column(String)       # Tovar Holati (masalan: "11-15 kunlik (85%)")
    # -------------------------------------

    # Sanalar
    import_date = Column(Date)          # latest_import_sana
    created_at = Column(Date, server_default=func.current_date()) # Faqat sana

    status = Column(String, default='Kutilmoqda')
class SupplierNameHistory(Base):
    __tablename__ = 'supplier_name_history'
    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, nullable=False, index=True)
    old_name = Column(String, nullable=False)
    new_name = Column(String, nullable=False)
    change_date = Column(DateTime(timezone=True), server_default=func.now())

# --- DB SESSIYASI ---
engine = create_engine(config.POSTGRES_URL)
Session = sessionmaker(bind=engine)

# --- FUNKSIYALAR ---

def init_db():
    """Barcha jadvallarni PostgreSQL da yaratadi va standart sozlamalarni kiritadi."""
    try:
        # Jadvallarni yaratish
        Base.metadata.create_all(engine)

        # Standart sozlamalar
        session = Session()
        if session.query(Setting).count() == 0:
            print("⚙️ Standart sozlamalar kiritilmoqda...")
            defaults = [
                Setting(rule_name='m1_min_days', rule_value=16.0),
                Setting(rule_name='m1_max_days', rule_value=20.0),
                Setting(rule_name='m1_percentage', rule_value=80.0),
                Setting(rule_name='m2_min_days', rule_value=11.0),
                Setting(rule_name='m2_max_days', rule_value=15.0),
                Setting(rule_name='m2_percentage', rule_value=70.0),
                Setting(rule_name='m3_min_days', rule_value=6.0),
                Setting(rule_name='m3_max_days', rule_value=10.0),
                Setting(rule_name='m3_percentage', rule_value=60.0),
                Setting(rule_name='m4_min_days', rule_value=1.0),
                Setting(rule_name='m4_max_days', rule_value=5.0),
                Setting(rule_name='m4_percentage', rule_value=30.0),
            ]
            session.add_all(defaults)
            session.commit()
        session.close()
        print("✅ PostgreSQL bazasi to'liq tayyor (SQLite ishlatilmaydi).")
    except Exception as e:
        print(f"❌ Bazani yaratishda xatolik: {e}")

def get_suppliers_with_orders() -> set:
    """generated_orders jadvalidan zakazlari mavjud bo'lgan yetkazib beruvchilar ro'yxati."""
    try:
        # Postgresdan o'qiymiz
        df = pd.read_sql("SELECT DISTINCT supplier FROM generated_orders WHERE status = 'Kutilmoqda'", engine)
        if not df.empty:
            return set(df['supplier'].str.replace('\u00A0', ' ', regex=False).str.strip().dropna())
        return set()
    except Exception as e:
        print(f"Xatolik (get_suppliers): {e}")
        return set()

def get_unassigned_suppliers() -> list[str]:
    """Ro'yxatdan o'tmagan VA zakazi bor supplierlar."""
    suppliers_with_orders = get_suppliers_with_orders()
    if not suppliers_with_orders:
        return []

    session = Session()
    try:
        assigned_suppliers = {s.name for s in session.query(Supplier).all()}
        unassigned = suppliers_with_orders - assigned_suppliers
        return sorted(list(unassigned))
    finally:
        session.close()

def is_admin(telegram_id: int) -> bool:
    session = Session()
    try:
        return session.query(Admin).filter_by(telegram_id=telegram_id).first() is not None
    finally:
        session.close()

def invite_users(telegram_ids: list[int]) -> tuple[int, int]:
    session = Session()
    added_count, exist_count = 0, 0
    try:
        for tid in telegram_ids:
            if not session.query(InvitedUser).filter_by(telegram_id=tid).first():
                session.add(InvitedUser(telegram_id=tid))
                added_count += 1
            else:
                exist_count += 1
        session.commit()
        return added_count, exist_count
    finally:
        session.close()

def check_invitation(telegram_id: int) -> InvitedUser | None:
    session = Session()
    try:
        return session.query(InvitedUser).filter_by(telegram_id=telegram_id, is_registered=False).first()
    finally:
        session.close()

def register_supplier(telegram_id: int, supplier_name: str) -> bool:
    session = Session()
    try:
        cleaned_name = supplier_name.replace('\u00A0', ' ').strip()
        new_supplier = Supplier(name=cleaned_name, telegram_id=telegram_id)
        session.add(new_supplier)
        invited_user = session.query(InvitedUser).filter_by(telegram_id=telegram_id).first()
        if invited_user:
            invited_user.is_registered = True
        session.commit()
        return True
    except IntegrityError:
        session.rollback()
        return False
    finally:
        session.close()

def get_supplier_by_id(telegram_id: int) -> Supplier | None:
    session = Session()
    try:
        return session.query(Supplier).filter_by(telegram_id=telegram_id).first()
    finally:
        session.close()

def update_supplier_name(telegram_id: int, new_name: str) -> tuple[bool, str | None]:
    session = Session()
    try:
        cleaned_new_name = new_name.replace('\u00A0', ' ').strip()
        if session.query(Supplier).filter_by(name=cleaned_new_name).first():
            return False, None

        supplier = session.query(Supplier).filter_by(telegram_id=telegram_id).first()
        if not supplier:
            return False, None

        old_name = supplier.name
        history_log = SupplierNameHistory(
            telegram_id=telegram_id,
            old_name=old_name,
            new_name=cleaned_new_name
        )
        session.add(history_log)
        supplier.name = cleaned_new_name
        session.commit()
        return True, old_name
    except IntegrityError:
        session.rollback()
        return False, None
    finally:
        session.close()

def get_all_settings() -> dict:
    session = Session()
    try:
        settings = session.query(Setting).all()
        return {s.rule_name: s.rule_value for s in settings}
    finally:
        session.close()

def update_setting(rule_name: str, new_value: float) -> bool:
    session = Session()
    try:
        setting = session.query(Setting).filter_by(rule_name=rule_name).first()
        if not setting:
            return False
        setting.rule_value = new_value
        session.commit()
        return True
    except Exception:
        session.rollback()
        return False
    finally:
        session.close()

def update_order_status(zakaz_id: str, new_status: str) -> bool:
    """Postgres dagi GeneratedOrder statusini yangilaydi."""
    session = Session()
    try:
        # zakaz_id bizda Artikul sifatida saqlangan (yoki generated_orders.zakaz_id)
        # Hamma bir xil zakaz_id (artikul) ga ega va "Kutilmoqda" bo'lgan qatorlarni yangilaymiz
        orders = session.query(GeneratedOrder).filter_by(zakaz_id=zakaz_id, status='Kutilmoqda').all()

        if not orders:
            return False

        for order in orders:
            order.status = new_status

        session.commit()
        return True
    except Exception as e:
        print(f"❌ Status yangilashda xatolik: {e}")
        session.rollback()
        return False
    finally:
        session.close()

def get_full_report_data() -> pd.DataFrame:
    """Hisobot uchun generated_orders jadvalidan BARCHA ustunlarni oladi."""
    try:
        # --- O'ZGARISH: Aniq ustunlar o'rniga (*) qo'ydik ---
        # Bu barcha ustunlarni (supply_price, prodano, hozirgi_qoldiq va h.k) olib keladi
        query = "SELECT * FROM generated_orders ORDER BY created_at DESC"

        df = pd.read_sql(query, engine)

        # Texnik 'id' ustuni excelda kerak bo'lmasa, olib tashlaymiz
        if 'id' in df.columns:
            df = df.drop(columns=['id'])

        # Ustunlar nomini chiroyli qilish (Ixtiyoriy)
        rename_map = {
            'zakaz_id': 'Zakaz ID',
            'supplier': 'Yetkazib beruvchi',
            'artikul': 'Artikul',
            'category': 'Kategoriya',
            'subcategory': 'Podkategoriya',
            'shop': 'Do\'kon',
            'color': 'Rang (Sana)',
            'quantity': 'Pochka Soni',
            'supply_price': 'Narx (Tanx)',
            'hozirgi_qoldiq': 'Qoldiq',
            'prodano': 'Yangi Sotuv',
            'days_passed': 'Kun o\'tdi',
            'ortacha_sotuv': 'O\'rtacha kunlik',
            'kutilyotgan_sotuv': 'Haftalik prognoz',
            'tovar_holati': 'Holat',
            'import_date': 'Kelgan Sana',
            'created_at': 'Yaratilgan Sana',
            'status': 'Status'
        }
        df = df.rename(columns=rename_map)

        return df
    except Exception as e:
        print(f"❌ Hisobot olishda xatolik: {e}")
        return pd.DataFrame()

def get_pending_orders_for_reminder(hours: int = 24) -> list:
    """Eslatma yuborish uchun Postgresdan ma'lumot oladi."""
    pending_orders = []
    session = Session()
    try:
        now_tashkent = datetime.now(TASHKENT_TZ).replace(tzinfo=None)
        time_threshold = now_tashkent - timedelta(hours=hours)
        # 1. Vaqti o'tgan va javob berilmagan zakazlarni olish
        orders = session.query(GeneratedOrder).filter(
            GeneratedOrder.status == 'Kutilmoqda',
            GeneratedOrder.created_at < time_threshold
        ).all()

        if not orders:
            return []

        # 2. Ularning supplierlarini aniqlash (SQLAlchemy object to list of dicts)
        unique_reminders = {} # (telegram_id, artikul) -> data

        # Supplier nomlarini tezroq topish uchun map
        supplier_names = {o.supplier for o in orders}
        suppliers_db = session.query(Supplier).filter(Supplier.name.in_(supplier_names)).all()
        supplier_map = {s.name: s.telegram_id for s in suppliers_db}

        for order in orders:
            tid = supplier_map.get(order.supplier)
            if tid:
                key = (tid, order.artikul)
                if key not in unique_reminders:
                    unique_reminders[key] = {
                        "telegram_id": tid,
                        "artikul": order.artikul,
                        "subcategory": order.subcategory
                    }

        return list(unique_reminders.values())

    except Exception as e:
        print(f"❌ Eslatmalarni olishda xatolik: {e}")
        return []
    finally:
        session.close()


def get_unassigned_categories() -> list[str]:
    """Hali egasi yo'q (ro'yxatdan o'tmagan) supplierlarga tegishli kategoriyalarni qaytaradi."""
    session = Session()
    try:
        # 1. Ro'yxatdan o'tgan supplier nomlarini olamiz
        registered_names = session.query(Supplier.name).all()
        registered_list = [r[0] for r in registered_names]

        # 2. generated_orders dan registered_list da YO'Q bo'lgan supplierlarning kategoriyalarini olamiz
        query = session.query(GeneratedOrder.category).filter(
            GeneratedOrder.category != None,
            GeneratedOrder.supplier.notin_(registered_list),
            GeneratedOrder.status == 'Kutilmoqda'
        ).distinct()

        return sorted([r[0] for r in query.all() if r[0]])
    finally:
        session.close()

def get_unassigned_subcategories(category_name: str) -> list[str]:
    """Tanlangan kategoriyaga tegishli, egasi yo'q podkategoriyalarni qaytaradi."""
    session = Session()
    try:
        registered_names = session.query(Supplier.name).all()
        registered_list = [r[0] for r in registered_names]

        query = session.query(GeneratedOrder.subcategory).filter(
            GeneratedOrder.category == category_name,
            GeneratedOrder.supplier.notin_(registered_list),
            GeneratedOrder.status == 'Kutilmoqda'
        ).distinct()

        return sorted([r[0] for r in query.all() if r[0]])
    finally:
        session.close()

def get_unassigned_suppliers_by_filter(category: str, subcategory: str) -> list[str]:
    """Kategoriya va Podkategoriya bo'yicha filtrlab, bo'sh supplierlarni qaytaradi."""
    session = Session()
    try:
        registered_names = session.query(Supplier.name).all()
        registered_list = [r[0] for r in registered_names]

        query = session.query(GeneratedOrder.supplier).filter(
            GeneratedOrder.category == category,
            GeneratedOrder.subcategory == subcategory,
            GeneratedOrder.supplier.notin_(registered_list),
            GeneratedOrder.status == 'Kutilmoqda'
        ).distinct()

        # Supplier nomini tozalab qaytaramiz
        suppliers = set()
        for row in query.all():
            if row[0]:
                suppliers.add(row[0].replace('\u00A0', ' ').strip())

        return sorted(list(suppliers))
    finally:
        session.close()
