import pandas as pd
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, Column, Integer, String, BigInteger, Boolean, Float, DateTime, Date, text, func
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import IntegrityError
import config

# --- VAQT SOZLAMASI (TASHKENT UTC+5) ---
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
    # --- O'ZGARISH: unique=True OLIB TASHLANDI ---
    # Endi bir xil nomda bir nechta odam bo'lishi mumkin
    name = Column(String, nullable=False) 
    telegram_id = Column(BigInteger, unique=True, nullable=False)
# --- VIP (RUXSAT BERILGANLAR) QISMI ---

class AllowedUser(Base):
    """Tizim yopiq bo'lganda ham kira oladiganlar"""
    __tablename__ = 'allowed_users'
    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, unique=True)

def toggle_allow_user(telegram_id: int, allow: bool):
    """True = Ruxsat berish, False = Ruxsatni olish"""
    session = Session()
    try:
        user = session.query(AllowedUser).filter_by(telegram_id=telegram_id).first()
        if allow:
            if not user: session.add(AllowedUser(telegram_id=telegram_id))
        else:
            if user: session.delete(user)
        session.commit()
        return True
    except:
        session.rollback()
        return False
    finally:
        session.close()

def is_allowed(telegram_id: int) -> bool:
    session = Session()
    try:
        return session.query(AllowedUser).filter_by(telegram_id=telegram_id).first() is not None
    finally:
        session.close()
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
    zakaz_id = Column(String, index=True)
    supplier = Column(String)
    artikul = Column(String)
    category = Column(String)
    subcategory = Column(String)
    shop = Column(String)
    color = Column(String)
    photo = Column(String)
    supply_price = Column(Float, default=0.0)
    quantity = Column(Integer)
    hozirgi_qoldiq = Column(Float)
    prodano = Column(Float)
    days_passed = Column(Integer)
    ortacha_sotuv = Column(Float)
    kutilyotgan_sotuv = Column(Float)
    tovar_holati = Column(String)
    import_date = Column(Date)
    created_at = Column(Date, server_default=func.current_date())
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
    try:
        Base.metadata.create_all(engine)
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
        print("✅ PostgreSQL bazasi to'liq tayyor.")
    except Exception as e:
        print(f"❌ Bazani yaratishda xatolik: {e}")

def get_suppliers_with_orders() -> set:
    try:
        df = pd.read_sql("SELECT DISTINCT supplier FROM generated_orders WHERE status = 'Kutilmoqda'", engine)
        if not df.empty:
            return set(df['supplier'].str.replace('\u00A0', ' ', regex=False).str.strip().dropna())
        return set()
    except Exception as e:
        print(f"Xatolik (get_suppliers): {e}")
        return set()

def get_unassigned_suppliers() -> list[str]:
    return sorted(list(get_suppliers_with_orders()))

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
        
        # Nom bandligini tekshirmaymiz, shunchaki yangisini qo'shamiz yoki yangilaymiz
        existing_supplier = session.query(Supplier).filter_by(telegram_id=telegram_id).first()
        if existing_supplier:
            existing_supplier.name = cleaned_name
        else:
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
        supplier = session.query(Supplier).filter_by(telegram_id=telegram_id).first()
        if not supplier: return False, None

        old_name = supplier.name
        
        # Tarixga yozish (log uchun)
        history_log = SupplierNameHistory(
            telegram_id=telegram_id, old_name=old_name, new_name=cleaned_new_name
        )
        session.add(history_log)
        
        # --- O'ZGARISH SHU YERDA ---
        # 1. Faqat Supplier (Foydalanuvchi) ismini yangilaymiz.
        # U endi yangi nomdagi zakazlarni ko'radi.
        supplier.name = cleaned_new_name
        
        # 2. generated_orders JADVALIGA TEGMAYMIZ! 
        # (Eski kodda bu yerda session.execute(...) bor edi, uni o'chirdik).
        
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
        if not setting: return False
        setting.rule_value = new_value
        session.commit()
        return True
    except Exception:
        session.rollback()
        return False
    finally:
        session.close()

def update_order_status(zakaz_id: str, new_status: str) -> bool:
    session = Session()
    try:
        orders = session.query(GeneratedOrder).filter_by(zakaz_id=zakaz_id, status='Kutilmoqda').all()
        if not orders: return False
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
    try:
        query = "SELECT * FROM generated_orders ORDER BY created_at DESC"
        df = pd.read_sql(query, engine)
        if 'id' in df.columns: df = df.drop(columns=['id'])
        
        rename_map = {
            'zakaz_id': 'Zakaz ID', 'supplier': 'Yetkazib beruvchi', 'artikul': 'Artikul',
            'category': 'Kategoriya', 'subcategory': 'Podkategoriya', 'shop': 'Do\'kon',
            'color': 'Rang (Sana)', 'quantity': 'Pochka Soni', 'supply_price': 'Sotuv Narxi',
            'hozirgi_qoldiq': 'Qoldiq', 'prodano': 'Yangi Sotuv', 'days_passed': 'Kun o\'tdi',
            'ortacha_sotuv': 'O\'rtacha kunlik', 'kutilyotgan_sotuv': 'Haftalik prognoz',
            'tovar_holati': 'Holat', 'import_date': 'Kelgan Sana', 'created_at': 'Yaratilgan Sana',
            'status': 'Status'
        }
        return df.rename(columns=rename_map)
    except Exception as e:
        print(f"❌ Hisobot olishda xatolik: {e}")
        return pd.DataFrame()

def get_pending_orders_for_reminder(hours: int = 24) -> list:
    session = Session()
    try:
        now_tashkent = datetime.now(TASHKENT_TZ).replace(tzinfo=None)
        time_threshold = now_tashkent - timedelta(hours=hours)
        
        orders = session.query(GeneratedOrder).filter(
            GeneratedOrder.status == 'Kutilmoqda',
            GeneratedOrder.created_at < time_threshold.date()
        ).all()

        if not orders: return []

        unique_reminders = {}
        supplier_names = {o.supplier for o in orders}
        suppliers_db = session.query(Supplier).filter(Supplier.name.in_(supplier_names)).all()
        
        supplier_map = {}
        for s in suppliers_db:
            if s.name not in supplier_map: supplier_map[s.name] = []
            supplier_map[s.name].append(s.telegram_id)

        for order in orders:
            tids = supplier_map.get(order.supplier, [])
            for tid in tids:
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

# --- FILTRLASH FUNKSIYALARI (TO'G'RILANGAN) ---

def get_unassigned_categories() -> list[str]:
    session = Session()
    try:
        # --- O'ZGARISH: registered_list FILTRINI OLIB TASHLADIK ---
        query = session.query(GeneratedOrder.category).filter(
            GeneratedOrder.category != None,
            GeneratedOrder.status == 'Kutilmoqda'
        ).distinct()
        return sorted([r[0] for r in query.all() if r[0]])
    finally:
        session.close()

def get_unassigned_subcategories(category_name: str) -> list[str]:
    session = Session()
    try:
        # --- O'ZGARISH: registered_list FILTRINI OLIB TASHLADIK ---
        query = session.query(GeneratedOrder.subcategory).filter(
            GeneratedOrder.category == category_name,
            GeneratedOrder.status == 'Kutilmoqda'
        ).distinct()
        return sorted([r[0] for r in query.all() if r[0]])
    finally:
        session.close()

def get_unassigned_suppliers_by_filter(category: str, subcategory: str) -> list[str]:
    """
    Kategoriya va Podkategoriya bo'yicha filtrlab, supplierlarni qaytaradi.
    """
    session = Session()
    try:
        cat_clean = category.strip()
        sub_clean = subcategory.strip()

        query = session.query(GeneratedOrder.supplier).filter(
            GeneratedOrder.category == cat_clean,
            GeneratedOrder.subcategory == sub_clean,
            GeneratedOrder.status == 'Kutilmoqda'
        ).distinct()

        suppliers = set()
        for row in query.all():
            if row[0]:
                clean_name = row[0].replace('\u00A0', ' ').strip()
                suppliers.add(clean_name)

        return sorted(list(suppliers))
    except Exception as e:
        print(f"❌ Filtrda xatolik: {e}")
        return []
    finally:
        session.close()




def get_supplier_stats_detailed(telegram_id: int):
    """
    Yetkazib beruvchi uchun: Kategoriya va Podkategoriya bo'yicha pochkalarni hisoblaydi.
    """
    session = Session()
    try:
        # 1. Yetkazib beruvchi nomini aniqlaymiz
        supplier = session.query(Supplier).filter_by(telegram_id=telegram_id).first()
        if not supplier:
            return []

        # 2. Faqat shu odamga tegishli zakazlarni guruhlab olamiz
        results = session.query(
            GeneratedOrder.category,
            GeneratedOrder.subcategory,
            func.sum(GeneratedOrder.quantity)
        ).filter(
            GeneratedOrder.supplier == supplier.name,
            GeneratedOrder.status == 'Kutilmoqda'
        ).group_by(
            GeneratedOrder.category,
            GeneratedOrder.subcategory
        ).all()
        
        return results # Qaytaradi: [('Shim', 'Jinsi', 5), ('Shim', 'Slaks', 3)...]
    finally:
        session.close()




# --- ADMIN STATISTIKASI UCHUN YANGI FUNKSIYALAR ---

def get_stat_categories_global():
    """Faqat 'Kutilmoqda' statusidagi bor Kategoriyalar ro'yxatini qaytaradi"""
    session = Session()
    try:
        query = session.query(GeneratedOrder.category).filter(
            GeneratedOrder.status == 'Kutilmoqda'
        ).distinct()
        return sorted([r[0] for r in query.all() if r[0]])
    finally:
        session.close()

def get_stat_subcategories_global(category):
    """Tanlangan Kategoriya ichidagi Podkategoriyalar ro'yxati"""
    session = Session()
    try:
        query = session.query(GeneratedOrder.subcategory).filter(
            GeneratedOrder.category == category,
            GeneratedOrder.status == 'Kutilmoqda'
        ).distinct()
        return sorted([r[0] for r in query.all() if r[0]])
    finally:
        session.close()

def get_stat_total_packs(category, subcategory):
    """Aniq bir turdagi tovarning umumiy pochka soni"""
    session = Session()
    try:
        total = session.query(func.sum(GeneratedOrder.quantity)).filter(
            GeneratedOrder.category == category,
            GeneratedOrder.subcategory == subcategory,
            GeneratedOrder.status == 'Kutilmoqda'
        ).scalar()
        return total or 0
    finally:
        session.close()


# --- YANGI QO'SHILGAN: IMPORT TAHLILI FUNKSIYALARI ---

def get_stats_by_import_days(min_day, max_day, category=None):
    """
    Kun oralig'i bo'yicha GLOBAL qidiruv.
    Supplier nomiga qarab filtrlamaydi!
    """
    session = Session()
    try:
        query = session.query(GeneratedOrder).filter(
            GeneratedOrder.status == 'Kutilmoqda',
            GeneratedOrder.days_passed >= min_day,
            GeneratedOrder.days_passed <= max_day
        )

        if category is None:
            # Kategoriyalarni olish
            results = query.with_entities(GeneratedOrder.category).distinct().all()
        else:
            # Podkategoriyalarni olish
            results = query.filter(GeneratedOrder.category == category).with_entities(GeneratedOrder.subcategory).distinct().all()

        return sorted([r[0] for r in results if r[0]])
    finally:
        session.close()

def get_import_orders_detailed(min_day, max_day, category, subcategory):
    """
    Kartochkalar chiqarish uchun kerakli BARCHA ma'lumotni 
    Pandas DataFrame shaklida qaytaradi.
    """
    try:
        query = """
        SELECT * FROM generated_orders 
        WHERE status = 'Kutilmoqda' 
          AND days_passed >= %(min_d)s 
          AND days_passed <= %(max_d)s
          AND category = %(cat)s
          AND subcategory = %(sub)s
        """
        params = {
            "min_d": min_day, 
            "max_d": max_day, 
            "cat": category, 
            "sub": subcategory
        }
        
        return pd.read_sql(query, engine, params=params)
    except Exception as e:
        print(f"❌ Import detallarini olishda xatolik: {e}")
        return pd.DataFrame()
# --- YANGI QISM: BLOKLASH VA GLOBAL QULF ---

# Yangi jadval: Bloklanganlar
class BlockedUser(Base):
    __tablename__ = 'blocked_users'
    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, unique=True)

# 1. Botni yopish/ochish (Sozlamalar jadvaliga yozamiz)
def set_global_lock(is_locked: bool):
    """True = Yopish, False = Ochish"""
    val = 1.0 if is_locked else 0.0
    # update_setting funksiyasi bor edi, shundan foydalanamiz
    if not update_setting('global_lock', val):
        # Agar yo'q bo'lsa yangi yaratamiz
        session = Session()
        session.add(Setting(rule_name='global_lock', rule_value=val))
        session.commit()
        session.close()

def is_global_locked() -> bool:
    settings = get_all_settings()
    return settings.get('global_lock', 0.0) == 1.0

# 2. Userni bloklash
def toggle_block_user(telegram_id: int, block: bool):
    session = Session()
    try:
        user = session.query(BlockedUser).filter_by(telegram_id=telegram_id).first()
        if block:
            if not user: session.add(BlockedUser(telegram_id=telegram_id))
        else:
            if user: session.delete(user)
        session.commit()
        return True
    except:
        return False
    finally:
        session.close()

def is_blocked(telegram_id: int) -> bool:
    session = Session()
    try:
        return session.query(BlockedUser).filter_by(telegram_id=telegram_id).first() is not None
    finally:
        session.close()
