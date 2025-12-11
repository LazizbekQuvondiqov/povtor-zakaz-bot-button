# data_engine.py
import math

from datetime import datetime, timedelta, timezone

from sqlalchemy import text
import time
import pandas as pd
import requests
import json
import os
import time
import warnings
import db_manager

import config


TASHKENT_TZ = timezone(timedelta(hours=5))
# Pandas'ning keraksiz ogohlantirishlarini o'chirish
warnings.simplefilter(action='ignore', category=UserWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)


# --- 1-QISM: YORDAMCHI FUNKSIYALAR: MA'LUMOTLARNI TOZALASH ---

def process_and_clean_sales_chunk(chunk_of_records):
    """Sotuvlar haqidagi xom ma'lumotlar qismini tozalab, tayyor DataFrame qaytaradi."""
    if not chunk_of_records:
        return pd.DataFrame()

    df = pd.DataFrame(chunk_of_records)

    rename_cols = {
        "product_id": "product_id", "product_sku": "Артикул", "product_name": "Наименование",
        "categories_path": "Категория", "product_brand_name": "Бренд", "product_barcode": "Баркод",
        "date": "Дата", "shop_name": "Магазин", "sold_measurement_value": "Кол-во проданных",
        "returned_measurement_value": "Кол-во возвращенных", "net_sold_measurement_value": "Продано за вычетом возвратов",
        "gross_sales": "Продажи без учета скидки", "returned_sales_sum": "Сумма возвратов",
        "net_sales": "Продажи со скидкой с учетом возвратов", "sold_supply_sum": "Продажи по цене закупки",
        "net_profit": "Валовая прибыль", "discount": "Скидка", "sold_with_discount": "Цена продажи"
    }
    df = df.rename(columns=rename_cols)

    def extract_custom_field(custom_fields_list, field_name):
        if isinstance(custom_fields_list, list):
            for field in custom_fields_list:
                if isinstance(field, dict) and field.get('custom_field_name') == field_name:
                    return field.get('custom_field_value')
        return None

    if 'custom_fields' in df.columns:
        df['Материал'] = df['custom_fields'].apply(lambda x: extract_custom_field(x, 'Материал'))
        df['Вид'] = df['custom_fields'].apply(lambda x: extract_custom_field(x, 'Вид'))
        df['Крой'] = df['custom_fields'].apply(lambda x: extract_custom_field(x, 'Крой'))
        df['Дата2'] = df['custom_fields'].apply(lambda x: extract_custom_field(x, 'Дата'))
        df['Акция'] = df['custom_fields'].apply(lambda x: extract_custom_field(x, 'Акция'))
        df['Подкатегория'] = df['custom_fields'].apply(lambda x: extract_custom_field(x, 'Подкатегория'))
        df['Модель'] = df['custom_fields'].apply(lambda x: extract_custom_field(x, 'Модель'))
        df = df.drop(columns=['custom_fields'])

    required_columns = [
        "product_id", 'Бренд', 'Материал', 'Вид', 'Категория', 'Наименование', 'Магазин', 'Дата', 'Дата2',
        'Артикул', 'Баркод', 'Подкатегория', 'Акция', 'Модель', 'Кол-во проданных', 'Кол-во возвращенных',
        'Продано за вычетом возвратов', 'Крой', 'Продажи без учета скидки', 'Сумма возвратов',
        'Продажи со скидкой с учетом возвратов', 'Продажи по цене закупки', 'Валовая прибыль', 'Скидка', 'Цена продажи'
    ]

    existing_columns = [col for col in required_columns if col in df.columns]
    df_clean = df[existing_columns].copy()

    if 'Дата' in df_clean.columns:
        df_clean['Дата'] = pd.to_datetime(df_clean['Дата'], errors='coerce')

    if 'Категория' in df_clean.columns:
        df_clean['Категория'] = df_clean['Категория'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else (x if isinstance(x, str) else None))

    if 'product_id' in df_clean.columns and 'Магазин' in df_clean.columns:
        df_clean['ProductShop_Key'] = df_clean['product_id'].astype(str) + '_' + df_clean['Магазин'].astype(str)

    return df_clean

def process_and_clean_stock_chunk(chunk_of_records, report_date_str):
    if not chunk_of_records:
        return pd.DataFrame()

    df = pd.DataFrame(chunk_of_records)
    df['Дата'] = pd.to_datetime(report_date_str)

    def extract_custom_field(custom_fields, field_name):
        if isinstance(custom_fields, list):
            for field in custom_fields:
                if isinstance(field, dict) and field.get('custom_field_name') == field_name:
                    return field.get('custom_field_value')
        return None

    if 'product_custom_fields' in df.columns:
        df['Подкатегория'] = df['product_custom_fields'].apply(lambda x: extract_custom_field(x, 'Подкатегория'))
        df['Материал'] = df['product_custom_fields'].apply(lambda x: extract_custom_field(x, 'Материал'))
        df['Вид'] = df['product_custom_fields'].apply(lambda x: extract_custom_field(x, 'Вид'))
        df = df.drop(columns=['product_custom_fields'])

    column_mapping = {
        'product_id': 'product_id', 'categories_path': 'Категория', 'product_name': "Наименование",
        'product_sku': 'Артикул', 'product_barcode': 'Баркод', 'shop_name': 'Магазин',
        'measurement_value': 'Кол-во', 'supply_price': 'Цена поставки', 'retail_price': 'Цена продажи',
        'estimated_income': 'Сумма прибыли остатков', "product_brand_name": "Бренд"
    }
    df = df.rename(columns=column_mapping)

    if 'Категория' in df.columns:
        df['Категория'] = df['Категория'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None)

    required_columns = [
        'product_id', 'Бренд', 'Категория', 'Материал', 'Вид', "Наименование", 'Дата', 'Артикул', 'Подкатегория',
        'Баркод', 'Магазин', 'Кол-во', 'Цена поставки', 'Цена продажи', 'Сумма прибыли остатков'
    ]
    existing_columns = [col for col in required_columns if col in df.columns]
    df_clean = df[existing_columns].copy()

    if 'product_id' in df_clean.columns and 'Магазин' in df_clean.columns:
        df_clean['ProductShop_Key'] = df_clean['product_id'].astype(str) + '_' + df_clean['Магазин'].astype(str)

    return df_clean


# --- 2-QISM: MA'LUMOTLARNI YANGILASH FUNKSIYALARI ---

def get_billz_access_token():
    url = "https://api-admin.billz.ai/v1/auth/login"
    headers = {"accept": "application/json", "Content-Type": "application/json"}
    try:
        response = requests.post(url, json={"secret_token": config.BILLZ_SECRET_KEY}, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        print("✅ Billz API uchun yangi access_token olindi.")
        return data["data"]["access_token"]
    except requests.exceptions.RequestException as e:
        print(f"❌ XATOLIK: Billz API tokenini olishda muammo: {e}")
        return None

def update_catalog(access_token, engine):
    print("\n--- 1-QADAM: MAHSULOTLAR KATALOGI TO'LIQ YANGILANMOQDA (FULL RELOAD) ---")

    all_products = []
    page = 1

    print("⏳ Billz API dan barcha mahsulotlar yuklanmoqda...")

    while True:
        params = {"limit": 1000, "page": page}
        try:
            response = requests.get(
                "https://api-admin.billz.ai/v2/products",
                params=params,
                headers={"authorization": f"Bearer {access_token}"},
                timeout=60
            )
            response.raise_for_status()
            items = response.json().get("products", [])
            if not items:
                break
            all_products.extend(items)
            print(f"📄 Sahifa {page}: {len(items)} ta mahsulot yuklandi...")
            if len(items) < 1000:
                break
            page += 1

        except requests.RequestException as e:
            print(f"❌ Katalog yuklashda xatolik (Sahifa {page}): {e}")
            break

    if not all_products:
        print("⚠️ Katalog bo'sh yoki API dan ma'lumot kelmadi.")
        return

    print(f"✅ Jami {len(all_products)} ta mahsulot yuklab olindi.")

    # --- DATAFRAME TAYYORLASH (YANGI MANTIQ) ---
    processed_data = []

    def get_field(custom_fields, name):
        for f in custom_fields or []:
            if f.get('custom_field_name') == name: return f.get('custom_field_value', '')
        return ''

    def get_supplier_name(suppliers):
        return suppliers[0].get("name", "") if suppliers else ""

    for p in all_products:
        # Endi do'konlar bo'yicha aylanmaymiz! Faqat 1 ta qator olamiz.
        # Narxni birinchi duch kelgan do'kondan olamiz (ma'lumot uchun).
        shop_prices = p.get('shop_prices', [])
        first_shop = shop_prices[0] if shop_prices else {}
        
        rec = {
            'product_id': p.get('id', ''),
            'Артикул': p.get('sku', ''),
            'Баркод': p.get('barcode', ''),
            'Наименование': p.get('name', ''),
            'Бренд': p.get('brand_name', ''),
            'Категория': p.get('categories')[0].get('name', '') if p.get('categories') else '',
            'Фото': p.get('main_image_url_full', p.get('main_image_url', '')),
            'Материал': get_field(p.get('custom_fields'), 'Материал'),
            'Вид': get_field(p.get('custom_fields'), 'Вид'),
            'Подкатегория': get_field(p.get('custom_fields'), 'Подкатегория'),
            'Акция': get_field(p.get('custom_fields'), 'Акция'),
            'Модель': get_field(p.get('custom_fields'), 'Модель'),
            'Крой': get_field(p.get('custom_fields'), 'Крой'),
            'Дата1': get_field(p.get('custom_fields'), 'Дата'),
            'Цвет': get_field(p.get('custom_fields'), 'Цвет'),
            'Поставщик': get_supplier_name(p.get("suppliers")),
            # Narxlar (Faqat ma'lumot uchun)
            'Цена продажи': first_shop.get('retail_price', 0),
            'supply_price': first_shop.get('supply_price', 0)
        }
        # 'Магазин' va 'ProductShop_Key' ustunlari endi bu yerda YO'Q!
        processed_data.append(rec)

    if processed_data:
        d_mahsulotlar = pd.DataFrame(processed_data)

        # Dublikatlarni ID bo'yicha tozalaymiz (Ehtiyot shart)
        before_dedup = len(d_mahsulotlar)
        d_mahsulotlar.drop_duplicates(subset=['product_id'], keep='first', inplace=True)
        after_dedup = len(d_mahsulotlar)

        if before_dedup > after_dedup:
            print(f"🧹 {before_dedup - after_dedup} ta takroriy ID olib tashlandi.")

        d_mahsulotlar.to_sql("d_mahsulotlar", engine, if_exists="replace", index=False)
        print(f"✅ 'd_mahsulotlar' jadvali {len(d_mahsulotlar)} ta UNIKAL tovar bilan yangilandi.")
    else:
        print("⚠️ Qayta ishlashdan so'ng ma'lumotlar bo'sh qoldi.")

def update_sales(access_token, engine):
    print("\n--- 2-QADAM: SOTUVLARNI YANGILASH (KUNMA-KUN) ---")

    end_date = datetime.now(TASHKENT_TZ).replace(tzinfo=None)
    start_date = end_date - timedelta(days=23)

    try:
        with engine.connect() as conn:

            has_table = conn.execute(text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'f_sotuvlar')")).scalar()

            if has_table:
                result = conn.execute(text('SELECT MAX("Дата") FROM f_sotuvlar')).scalar()
                if result:
                    last_date_in_db = pd.to_datetime(result)
                    print(f"📅 Bazadagi oxirgi sana: {last_date_in_db.strftime('%Y-%m-%d')}")

                    start_date = last_date_in_db
                else:
                    print("⚠️ Jadval bor, lekin ichi bo'sh. 23 kunlik yuklanadi.")
            else:
                print("⚠️ Jadval yo'q. 23 kunlik yuklanadi.")
    except Exception as e:
        print(f"⚠️ Sanani aniqlashda xatolik: {e}. Standart 23 kun olinadi.")

    current_process_date = start_date

    while current_process_date <= end_date:
        day_str = current_process_date.strftime("%Y-%m-%d")
        print(f"⏳ {day_str} uchun ma'lumot olinmoqda...")

        page = 1
        day_chunks = []


        while True:
            try:
                while True:
                    params = {
                        "start_date": day_str,
                        "end_date": day_str,
                        "page": page,
                        "limit": 1000,
                        "shop_ids": config.ALL_SHOPS_IDS,
                        "currency": "UZS",
                        "detalization_by_position": "true"
                    }
                    response = requests.get(
                        "https://api-admin.billz.ai/v1/product-general-table",
                        headers={"Authorization": f"Bearer {access_token}"},
                        params=params,
                        timeout=60
                    )
                    response.raise_for_status()
                    records = response.json().get('products_stats_by_date', [])

                    if not records:
                        break

                    day_chunks.append(process_and_clean_sales_chunk(records))
                    if len(records) < 1000:
                        break
                    page += 1
                break
            except Exception as e:
                print(f"❌ {day_str} da API xatosi: {e}. 60 soniya kutib qayta urinamiz...")
                time.sleep(60)
                page = 1
                day_chunks = []


        if day_chunks:
            daily_df = pd.concat(day_chunks, ignore_index=True)
        
            try:
                # 1. Alohida sessiya ochib o‘chirishga urinamiz
                try:
                    with engine.begin() as conn:
                        delete_query = text(f'''
                        DELETE FROM f_sotuvlar 
                        WHERE "Дата" >= '{day_str} 00:00:00' 
                        AND "Дата" <= '{day_str} 23:59:59'
                        ''')
                        conn.execute(delete_query)
                except Exception:
                    pass
        
                # 2. Yangi ma'lumotni yozamiz
                with engine.begin() as conn:
                    daily_df.to_sql("f_sotuvlar", conn, if_exists="append", index=False)
        
                print(f"✅ {day_str} muvaffaqiyatli yangilandi. ({len(daily_df)} qator)")
            except Exception as e:
                print(f"❌ {day_str} ni bazaga yozishda xatolik: {e}")
        
        else:
            print(f"ℹ️ {day_str} uchun sotuv yo‘q.")


        current_process_date += timedelta(days=1)


    cutoff_date = (end_date - timedelta(days=24)).strftime("%Y-%m-%d")
    try:
        with engine.begin() as conn:
            conn.execute(text(f'DELETE FROM f_sotuvlar WHERE "Дата" < \'{cutoff_date}\''))
        print(f"🗑 {cutoff_date} dan oldingi eski arxiv tozalandi.")
    except Exception:
        pass

def update_stock(access_token, engine):
    print("\n--- 3-QADAM: QOLDIQLARNI YANGILASH (KUNMA-KUN) ---")

    end_date = datetime.now(TASHKENT_TZ).replace(tzinfo=None)
    start_date = end_date - timedelta(days=23)
    try:
        with engine.connect() as conn:
            has_table = conn.execute(text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'f_qoldiqlar')")).scalar()
            if has_table:
                result = conn.execute(text('SELECT MAX("Дата") FROM f_qoldiqlar')).scalar()
                if result:
                    last_date_in_db = pd.to_datetime(result)
                    print(f"📅 Bazadagi oxirgi qoldiq sanasi: {last_date_in_db.strftime('%Y-%m-%d')}")
                    start_date = last_date_in_db
    except Exception as e:
        print(f"⚠️ Sana aniqlash xatosi: {e}")

    current_process_date = start_date
    
    while current_process_date <= end_date:
        day_str = current_process_date.strftime("%Y-%m-%d")
        print(f"⏳ {day_str} qoldiqlari olinmoqda...")

        day_chunks = []
        page = 1

        while True:
            try:
                while True:
                    params = {"report_date": day_str, "page": page, "limit": 1000, "shop_ids": config.ALL_SHOPS_IDS, "currency": "UZS"}
                    response = requests.get(
                        "https://api-admin.billz.ai/v1/stock-report-table",
                        headers={"Authorization": f"Bearer {access_token}"},
                        params=params,
                        timeout=60
                    )
                    response.raise_for_status()
                    records = response.json().get("rows", [])
                    if not records:
                        break
                    day_chunks.append(process_and_clean_stock_chunk(records, day_str))
                    if len(records) < 1000:
                        break
                    page += 1
                break
            except Exception as e:
                print(f"❌ {day_str} API xatosi: {e}. 60 soniya kutish...")
                time.sleep(60)
                page = 1
                day_chunks = []
        
        if day_chunks:
            daily_df = pd.concat(day_chunks, ignore_index=True)
        
            try:
                with engine.begin() as conn:
                    conn.execute(text(f'''DELETE FROM f_qoldiqlar WHERE "Дата" = '{day_str}' '''))
            except Exception:
                pass
        
            try:
                with engine.begin() as conn:
                    daily_df.to_sql("f_qoldiqlar", conn, if_exists="append", index=False)
                print(f"✅ {day_str} qoldiq yozildi.")
            except Exception as e:
                print(f"❌ {day_str} qoldiqni bazaga yozishda xatolik: {e}")


        current_process_date += timedelta(days=1)

    cutoff_date = (end_date - timedelta(days=24)).strftime("%Y-%m-%d")
    try:
        with engine.begin() as conn:
            conn.execute(text(f'DELETE FROM f_qoldiqlar WHERE "Дата" < \'{cutoff_date}\''))
        print(f"🗑 {cutoff_date} dan eski qoldiqlar tozalandi.")
    except Exception:
        pass

    # --- YANGI QISM: DO'KONLAR JADVALINI YANGILASH ---
    try:
        print("🏪 d_Magazinlar jadvali yangilanmoqda...")
        with engine.begin() as conn:
            # f_Qoldiqlar dan barcha unikal do'kon nomlarini olamiz
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS "d_Magazinlar" AS 
                SELECT DISTINCT "Магазин" FROM f_qoldiqlar
            """))
            # Agar oldin bor bo'lsa, yangi do'konlarni qo'shamiz
            conn.execute(text("""
                INSERT INTO "d_Magazinlar" ("Магазин")
                SELECT DISTINCT "Магазин" FROM f_qoldiqlar
                EXCEPT
                SELECT "Магазин" FROM "d_Magazinlar"
            """))
        print("✅ d_Magazinlar tayyor.")
    except Exception as e:
        print(f"⚠️ d_Magazinlar yangilashda xatolik: {e}")
        
def analyze_and_generate_orders(engine):
    print("\n--- 4-QADAM: TAHLIL (MAGAZIN + ARTIKUL + RANG + SANA FILTRI) ---")

    try:
        # ---------------------------------------------------------
        # 1. JADVALLARNI O'QISH
        # ---------------------------------------------------------
        
        # A) DIMENSION: Mahsulotlar (Import sanasi kerak)
        d_mahsulotlar = pd.read_sql("SELECT * FROM d_mahsulotlar", engine)
        
        # B) FACT: Sotuvlar (DIQQAT: "Дата" ustuni bilan birga olinyapti)
        f_sotuvlar = pd.read_sql('SELECT product_id, "Магазин", "Продано за вычетом возвратов", "Дата" FROM f_sotuvlar', engine)
        
        # C) FACT: Qoldiqlar (Snapshot - faqat bugungi holat)
        qoldiq_query = """
        SELECT t1.product_id, t1."Магазин", t1."Кол-во"
        FROM f_qoldiqlar t1
        INNER JOIN (
            SELECT "Магазин", MAX("Дата") as max_date
            FROM f_qoldiqlar
            GROUP BY "Магазин"
        ) t2 ON t1."Магазин" = t2."Магазин" AND t1."Дата" = t2.max_date
        """
        f_qoldiqlar = pd.read_sql(qoldiq_query, engine)

        # ---------------------------------------------------------
        # 2. FORMATLASH VA DATA TAYYORLASH
        # ---------------------------------------------------------
        f_sotuvlar['Магазин'] = f_sotuvlar['Магазин'].astype(str).str.strip()
        f_qoldiqlar['Магазин'] = f_qoldiqlar['Магазин'].astype(str).str.strip()
        
        d_mahsulotlar['product_id'] = d_mahsulotlar['product_id'].astype(str)
        f_sotuvlar['product_id'] = f_sotuvlar['product_id'].astype(str)
        f_qoldiqlar['product_id'] = f_qoldiqlar['product_id'].astype(str)
        
        # Sanalarni datetime formatiga o'tkazish (Taqqoslashtirish uchun o'ta muhim!)
        f_sotuvlar['sotuv_sanasi'] = pd.to_datetime(f_sotuvlar['Дата'], errors='coerce')
        
        # Import sanasini aniqlash
        date_col = 'import_date' if 'import_date' in d_mahsulotlar.columns else 'Дата1'
        d_mahsulotlar['import_sana_dt'] = pd.to_datetime(d_mahsulotlar[date_col], errors='coerce', dayfirst=True)
        # Sana yo'q bo'lsa, bugungi kun qo'yiladi (xato bermasligi uchun)
        d_mahsulotlar['import_sana_dt'].fillna(datetime.now(), inplace=True)
        d_mahsulotlar['Цвет'] = d_mahsulotlar['Цвет'].fillna('No Color')

        settings = db_manager.get_all_settings()

    except Exception as e:
        print(f"❌ Xatolik (O'qishda): {e}")
        return

    # ---------------------------------------------------------
    # 3. "ANCHOR DATE" (TAYANCH SANA) NI ANIQLASH
    # Har bir Magazin+Artikul+Rang uchun eng yangi import sanasini topamiz.
    # ---------------------------------------------------------
    
    # Qoldiqlarni mahsulot ma'lumotlari bilan birlashtiramiz
    qoldiq_merged = pd.merge(f_qoldiqlar, d_mahsulotlar, on='product_id', how='left')
    qoldiq_merged.dropna(subset=['Артикул'], inplace=True)

    # GURUHLASH 1: Har bir guruh uchun MAX import sanasini topish
    reference_dates = qoldiq_merged.groupby(['Артикул', 'Магазин', 'Цвет'], as_index=False)['import_sana_dt'].max()
    reference_dates.rename(columns={'import_sana_dt': 'max_import_date'}, inplace=True)

    # ---------------------------------------------------------
    # 4. SOTUVLARNI FILTRLASH (FILTR LOGIKASI)
    # Faqat "Sotuv Sanasi >= Import Sanasi" bo'lgan qatorlarni olamiz
    # ---------------------------------------------------------
    
    # Sotuvlarga mahsulot ma'lumotlarini ulaymiz
    sotuv_merged = pd.merge(f_sotuvlar, d_mahsulotlar[['product_id', 'Артикул', 'Цвет']], on='product_id', how='left')
    sotuv_merged.dropna(subset=['Артикул'], inplace=True)

    # Sotuvlarga boyagi "MAX SANA"ni ulaymiz
    sotuv_final = pd.merge(sotuv_merged, reference_dates, on=['Артикул', 'Магазин', 'Цвет'], how='left')
    
    # Agar max sana topilmasa (qoldiqda yo'q bo'lsa), sotuvni tashlab yuboramiz yoki hammasini olamiz.
    # Sizning holatda aktiv tovarlar muhim, shuning uchun sanasi borlarni olamiz.
    sotuv_final.dropna(subset=['max_import_date'], inplace=True)
    
    # 🔥 FILTR: Eski tarixni kesib tashlash
    # Andalus misoli: 9-Dekabrdan oldingi 13 ta sotuv shu yerda o'chib ketadi.
    sotuv_filtered = sotuv_final[sotuv_final['sotuv_sanasi'] >= sotuv_final['max_import_date']].copy()
    
    # GURUHLASH 2: Endi toza sotuvlarni SUM qilamiz
    sotuv_grp = sotuv_filtered.groupby(['Артикул', 'Магазин', 'Цвет'], as_index=False)['Продано за вычетом возвратов'].sum()
    sotuv_grp.rename(columns={'Продано за вычетом возвратов': 'Prodano'}, inplace=True)

    # ---------------------------------------------------------
    # 5. QOLDIQLARNI GURUHLASH (AGGREGATION)
    # ---------------------------------------------------------
    
    # Qoldiqlar uchun SUM va boshqa ma'lumotlarni (Narx, Rasm) olish
    agg_rules_qoldiq = {
        'Кол-во': 'sum',            # 1 pochka + 2 pochka = 3 pochka
        'import_sana_dt': 'max',    # Eng YANGI sana
        'supply_price': 'max',
        'Поставщик': 'first',
        'Категория': 'first',
        'Подкатегория': 'first',
        'Фото': 'first'
    }
    
    qoldiq_grp = qoldiq_merged.groupby(['Артикул', 'Магазин', 'Цвет'], as_index=False).agg(agg_rules_qoldiq)
    qoldiq_grp.rename(columns={'Кол-во': 'Hozirgi_Qoldiq'}, inplace=True)

    # ---------------------------------------------------------
    # 6. MASTER JADVALNI YIG'ISH VA HISOBLASH
    # ---------------------------------------------------------
    
    # Asosiy jadval - Qoldiqlar (Chunki biz bor tovarga zakaz beryapmiz)
    final_df = pd.merge(qoldiq_grp, sotuv_grp, on=['Артикул', 'Магазин', 'Цвет'], how='left')
    final_df['Prodano'].fillna(0, inplace=True) # Sotuv yo'q bo'lsa 0 bo'ladi

    # Kunlar farqini hisoblash
    max_sana_kalendar = datetime.now(TASHKENT_TZ).replace(tzinfo=None)
    final_df['days_passed'] = (max_sana_kalendar - final_df['import_sana_dt']).dt.days
    final_df['days_passed'] = final_df['days_passed'].clip(lower=0)

    # O'rtacha kunlik sotuv
    final_df['avg_sales'] = final_df.apply(
        lambda row: row['Prodano'] / (row['days_passed'] if row['days_passed'] > 0 else 1), axis=1
    )

    def calculate_order(row):
        kun = row['days_passed']
        sotuv = row['Prodano']          # Bu yerda endi faqat YANGI sotuvlar (Filtrlangan)
        qoldiq = row['Hozirgi_Qoldiq']  # Bu yerda JAMI qoldiq (SUM)
        avg = row['avg_sales']
        
        # Import soni = Yangi Sotuv + Jami Qoldiq
        import_soni = sotuv + qoldiq
        if import_soni == 0: return 0
        
        foiz = (sotuv / import_soni) * 100
        
        # --- QOIDALAR ---
        
        # 4-QOIDA (1-5 kun, Yangi)
        if settings.get('m4_min_days', 1) <= kun <= settings.get('m4_max_days', 5):
            if foiz >= settings.get('m4_percentage', 50):
                # Agar 3 ta sotilgan bo'lsa (va bu 50% dan ko'p bo'lsa) -> 3 ta zakaz beradi
                return sotuv * 1.0

        # 3-QOIDA (6-9 kun)
        if settings.get('m3_min_days', 6) <= kun <= settings.get('m3_max_days', 9):
            if foiz >= settings.get('m3_percentage', 70):
                return avg * 7 

        # 2-QOIDA (10-14 kun)
        if settings.get('m2_min_days', 10) <= kun <= settings.get('m2_max_days', 14):
            if foiz >= settings.get('m2_percentage', 85):
                return avg * 7

        # 1-QOIDA (15+ kun, Eski)
        if settings.get('m1_min_days', 15) <= kun <= settings.get('m1_max_days', 1000):
            if foiz >= settings.get('m1_percentage', 99):
                return avg * 7

        return 0

    final_df['final_order'] = final_df.apply(calculate_order, axis=1)
    
    # Pochka hisoblash (Sizning shkalangiz)
    def to_pochka(dona):
        dona = float(dona)
        if dona <= 2: return 0
        if dona <= 4: return 1  # 3 dona -> 1 pochka
        if dona <= 10: return 2
        if dona <= 15: return 3
        if dona <= 23: return 4
        if dona <= 29: return 5
        return math.ceil(dona / 6)

    orders = final_df[final_df['final_order'] > 0].copy()
    
    if orders.empty:
        print("✅ Zakaz yo'q.")
        return

    orders['quantity'] = orders['final_order'].apply(to_pochka).astype(int)
    orders = orders[orders['quantity'] > 0].copy()

    # Formatlash
    orders['sana_str'] = orders['import_sana_dt'].dt.strftime('%d.%m.%Y')
    orders['color'] = orders['Цвет'].astype(str) + " (" + orders['sana_str'] + ")"
    orders['tovar_holati'] = "Shart Bajarildi"

    # DB ga yozish uchun tayyorlash
    rename_map = {
        'Артикул': 'zakaz_id',
        'Поставщик': 'supplier',
        'Категория': 'category',
        'Подкатегория': 'subcategory',
        'Магазин': 'shop',
        'Фото': 'photo',
        'import_sana_dt': 'import_date',
        'Hozirgi_Qoldiq': 'hozirgi_qoldiq',
        'Prodano': 'prodano',
        'days_passed': 'days_passed',
        'avg_sales': 'ortacha_sotuv',
        'final_order': 'kutilyotgan_sotuv',
        'supply_price': 'supply_price'
    }
    
    orders_db = orders.rename(columns=rename_map)
    orders_db['artikul'] = orders_db['zakaz_id']
    orders_db['status'] = 'Kutilmoqda'
    orders_db['created_at'] = datetime.now(TASHKENT_TZ).replace(tzinfo=None).date()
    orders_db['import_date'] = pd.to_datetime(orders_db['import_date']).dt.date

    cols = [
        'zakaz_id', 'supplier', 'artikul', 'category', 'subcategory', 'shop', 'color', 'photo',
        'quantity', 'supply_price', 'hozirgi_qoldiq', 'prodano', 'days_passed', 
        'ortacha_sotuv', 'kutilyotgan_sotuv', 'tovar_holati', 'import_date', 'created_at', 'status'
    ]
    orders_db = orders_db[[c for c in cols if c in orders_db.columns]]

    try:
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM generated_orders WHERE status = 'Kutilmoqda'"))
            orders_db.to_sql("generated_orders", conn, if_exists="append", index=False)
        print(f"✅ BAZA YANGILANDI: {len(orders_db)} ta to'g'ri hisoblangan va guruhlangan zakaz yozildi.")
    except Exception as e:
        print(f"❌ Yozishda xatolik: {e}")

def run_full_update():
    """
    Barcha ma'lumotlarni yangilash jarayonini boshqaradi.
    To'liq PostgreSQL va 'Smart Update' (kunma-kun) rejimida ishlaydi.
    """
    start_time = time.time()
    print(f"\n--- 🚀 MA'LUMOTLARNI TO'LIQ YANGILASH BOSHLANDI: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")

    access_token = get_billz_access_token()
    if not access_token:
        print("❌ Yangilash to'xtatildi: Access token olinmadi.")
        return


    try:

        engine = db_manager.engine


        update_catalog(access_token, engine)


        update_sales(access_token, engine)


        update_stock(access_token, engine)


        analyze_and_generate_orders(engine)

    except Exception as e:
        print(f"🔥🔥🔥 YANGILASH JARAYONIDA JIDDIY XATOLIK YUZ BERDI: {e}")

    end_time = time.time()
    duration_minutes = (end_time - start_time) / 60
    print(f"\n🏁 --- JARAYON YAKUNLANDI. Umumiy vaqt: {duration_minutes:.2f} daqiqa ---")
