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

    # Eski kesh va sync fayllarni o'qimaymiz, har safar 0 dan olamiz.
    all_products = []
    page = 1

    print("⏳ Billz API dan barcha mahsulotlar yuklanmoqda...")

    while True:
        # last_updated_date PARAMETRI OLIB TASHLANDI
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

            if len(items) < 1000: # Agar 1000 tadan kam kelsa, demak oxirgi sahifa
                break
            page += 1

        except requests.RequestException as e:
            print(f"❌ Katalog yuklashda xatolik (Sahifa {page}): {e}")
            # Xatolik bo'lsa ham borini saqlashga harakat qilamiz yoki to'xtatamiz
            break

    if not all_products:
        print("⚠️ Katalog bo'sh yoki API dan ma'lumot kelmadi.")
        return

    print(f"✅ Jami {len(all_products)} ta mahsulot yuklab olindi.")

    # JSON faylni ham har safar yangilaymiz (Zahira uchun)
    try:
        with open(config.PRODUCTS_JSON_FILE, "w", encoding="utf-8") as f:
            json.dump(all_products, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠️ JSON saqlashda xatolik: {e}")

    # --- DATAFRAME TAYYORLASH ---
    processed_data = []
    TARGET_SHOPS = ['Dressco Integro', 'MAGNIT MEN', 'ANDALUS', 'BERUNIY MEN', 'SHAXRISTON']

    def get_field(custom_fields, name):
        for f in custom_fields or []:
            if f.get('custom_field_name') == name: return f.get('custom_field_value', '')
        return ''

    def get_supplier_name(suppliers):
        return suppliers[0].get("name", "") if suppliers else ""

    for p in all_products:
            base = {
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
                'supply_price': 0, # Boshlanishiga 0, pastda o'zgartiramiz
                'Поставщик': get_supplier_name(p.get("suppliers"))
            }
    
            # Har bir do'kon narxini tekshirish
            for shop in p.get('shop_prices') or []:
                shop_name = shop.get('shop_name', '').strip().upper()
                
                if shop_name in TARGET_SHOPS:
                    rec = base.copy()
                    rec['Магазин'] = shop_name
                    
                    # Sotuv narxini olamiz
                    narx = shop.get('retail_price', 0)
    
                    # 1. Asosiy narx ustuni (hisob-kitoblar uchun)
                    rec['Цена продажи'] = narx
                    
                    # 2. "Tanx narxi" o'rniga ham SOTUV NARXINI yozamiz (Botda ko'rinishi uchun)
                    rec['supply_price'] = narx
                    
                    processed_data.append(rec)

    if processed_data:
        d_mahsulotlar = pd.DataFrame(processed_data)


        d_mahsulotlar['ProductShop_Key'] = d_mahsulotlar['product_id'].astype(str) + '_' + d_mahsulotlar['Магазин'].astype(str)


        before_dedup = len(d_mahsulotlar)
        d_mahsulotlar.drop_duplicates(subset=['ProductShop_Key'], keep='first', inplace=True)
        after_dedup = len(d_mahsulotlar)

        if before_dedup > after_dedup:
            print(f"🧹 {before_dedup - after_dedup} ta dublikat qator tozalandi.")


        d_mahsulotlar.to_sql("d_mahsulotlar", engine, if_exists="replace", index=False)

        print(f"✅ 'd_mahsulotlar' jadvali {len(d_mahsulotlar)} ta yozuv bilan to'liq yangilandi.")
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

                with engine.begin() as conn:

                    delete_query = text(f'DELETE FROM f_sotuvlar WHERE "Дата" >= \'{day_str} 00:00:00\' AND "Дата" <= \'{day_str} 23:59:59\'')
                    conn.execute(delete_query)

                    daily_df.to_sql("f_sotuvlar", conn, if_exists="append", index=False)

                print(f"✅ {day_str} muvaffaqiyatli yangilandi. ({len(daily_df)} qator)")
            except Exception as e:
                print(f"❌ {day_str} ni bazaga yozishda xatolik: {e}")

        else:
            print(f"ℹ️ {day_str} uchun sotuv yo'q.")

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

                    conn.execute(text(f'DELETE FROM f_qoldiqlar WHERE "Дата" = \'{day_str}\''))

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

def analyze_and_generate_orders(engine):
    print("\n--- 4-QADAM: TAHLIL (SOTUV[max_date] / KUN[max_date] + TO'LIQ YANGILASH) ---")

    try:
        print("   📊 Ma'lumotlar o'qilmoqda...")
        f_sotuvlar = pd.read_sql("SELECT * FROM f_sotuvlar", engine)

        # Faqat oxirgi kundagi qoldiqni olamiz
        qoldiq_query = """
        SELECT * FROM f_qoldiqlar
        WHERE "Дата" = (SELECT MAX("Дата") FROM f_qoldiqlar)
        """
        f_qoldiqlar = pd.read_sql(qoldiq_query, engine)

        d_mahsulotlar = pd.read_sql("SELECT * FROM d_mahsulotlar", engine)
        settings = db_manager.get_all_settings()
    except Exception as e:
        print(f"❌ Xatolik: {e}")
        return

    if d_mahsulotlar.empty or f_sotuvlar.empty:
        print("⚠️ Ma'lumot yetarli emas.")
        return

    # --- 1. SANALARNI FORMATLASH ---
    f_sotuvlar['Дата'] = pd.to_datetime(f_sotuvlar['Дата'], errors='coerce')
    f_qoldiqlar['Дата'] = pd.to_datetime(f_qoldiqlar['Дата'], errors='coerce')

    d_mahsulotlar['import_sana'] = d_mahsulotlar['Дата1'].astype(str).str.replace('M-', '', regex=False)
    d_mahsulotlar['import_sana'] = pd.to_datetime(d_mahsulotlar['import_sana'], format='%d.%m.%Y', dayfirst=True, errors='coerce')

    # Kalitlarni tozalash
    d_mahsulotlar['ProductShop_Key'] = d_mahsulotlar['ProductShop_Key'].astype(str).str.strip()
    f_sotuvlar['ProductShop_Key'] = f_sotuvlar['ProductShop_Key'].astype(str).str.strip()
    f_qoldiqlar['ProductShop_Key'] = f_qoldiqlar['ProductShop_Key'].astype(str).str.strip()

    # Filtrlash (0 va 1 seriyalar kerak emas)
    d_mahsulotlar = d_mahsulotlar[~d_mahsulotlar['Артикул'].astype(str).str.startswith('0')]
    d_mahsulotlar = d_mahsulotlar[~d_mahsulotlar['Артикул'].astype(str).str.startswith('1')]

    # --- 2. MAX IMPORT DATE ---
    d_mahsulotlar['max_import_sana'] = d_mahsulotlar.groupby('Артикул')['import_sana'].transform('max')
    d_mahsulotlar.dropna(subset=['max_import_sana'], inplace=True)

    # --- 3. SOTUVNI FILTRLASH ---
    import_map = d_mahsulotlar[['ProductShop_Key', 'max_import_sana']].drop_duplicates(subset=['ProductShop_Key'])
    f_sotuvlar = f_sotuvlar.merge(import_map, on='ProductShop_Key', how='left')

    # QOIDA: Faqat Max Import Sanasidan KEYINGI (yoki teng) sotuvlarni olamiz
    f_sotuvlar_filtered = f_sotuvlar[f_sotuvlar['Дата'] >= f_sotuvlar['max_import_sana']].copy()

    print(f"   🧹 Toza sotuvlar soni: {len(f_sotuvlar_filtered)} (Eskilar olib tashlandi)")

    prodano = f_sotuvlar_filtered.groupby('ProductShop_Key')['Продано за вычетом возвратов'].sum().reset_index(name='Prodano')

    # --- 4. TAHLIL JADVALINI YIG'ISH ---
    df_analiz = d_mahsulotlar.copy()

    latest_qoldiq_date = f_qoldiqlar['Дата'].max()
    hozirgi_qoldiq = f_qoldiqlar[f_qoldiqlar['Дата'] == latest_qoldiq_date].groupby('ProductShop_Key')['Кол-во'].sum().reset_index(name='Hozirgi_Qoldiq')

    df_analiz = pd.merge(df_analiz, prodano, on='ProductShop_Key', how='left')
    df_analiz = pd.merge(df_analiz, hozirgi_qoldiq, on='ProductShop_Key', how='left')

    df_analiz['Prodano'] = df_analiz['Prodano'].fillna(0)
    df_analiz['Hozirgi_Qoldiq'] = df_analiz['Hozirgi_Qoldiq'].fillna(0)

    # --- 5. FORMULA QISMI ---
    max_sana_kalendar = datetime.now(TASHKENT_TZ).replace(tzinfo=None)
    df_analiz['Дней прошло'] = (max_sana_kalendar - df_analiz['max_import_sana']).dt.days

    df_analiz['o\'rtcha sotuv'] = df_analiz.apply(
        lambda row: row['Prodano'] / (row['Дней прошло'] if row['Дней прошло'] > 0 else 1), axis=1
    )

# 1 kunlik tovarlarni 3 ga, eskilarini 7 ga ko'paytiramiz
    df_analiz['kutulyotgan sotuv'] = df_analiz.apply(
        lambda row: row['o\'rtcha sotuv'] * (3 if row['Дней прошло'] <= 1 else 7),
        axis=1
    )
    # --- 6. STATUS VA FILTR ---
    def calculate_tovar_status(row):
        tovar_yoshi = row['Дней прошло']
        sotuv_soni = row['Prodano']
        qoldiq_soni = row['Hozirgi_Qoldiq']

        if pd.isna(tovar_yoshi): return "Mos Emas"

        umumiy_miqdor = sotuv_soni + qoldiq_soni
        if umumiy_miqdor == 0: return "Mos Emas"

        sotuv_foizi = (sotuv_soni / umumiy_miqdor) * 100

        if (settings.get('m1_min_days', 0) <= tovar_yoshi <= settings.get('m1_max_days', 0)) and \
           (sotuv_foizi >= settings.get('m1_percentage', 0)): return "Shart Bajarildi"
        if (settings.get('m2_min_days', 0) <= tovar_yoshi <= settings.get('m2_max_days', 0)) and \
           (sotuv_foizi >= settings.get('m2_percentage', 0)): return "Shart Bajarildi"
        if (settings.get('m3_min_days', 0) <= tovar_yoshi <= settings.get('m3_max_days', 0)) and \
           (sotuv_foizi >= settings.get('m3_percentage', 0)): return "Shart Bajarildi"
        if (settings.get('m4_min_days', 0) <= tovar_yoshi <= settings.get('m4_max_days', 0)) and \
           (sotuv_foizi >= settings.get('m4_percentage', 0)): return "Shart Bajarildi"

        return "Mos Emas"

    df_analiz['Tovar Statusi'] = df_analiz.apply(calculate_tovar_status, axis=1)
    hisobot_final = df_analiz[df_analiz['Tovar Statusi'] == "Shart Bajarildi"].copy()

    if hisobot_final.empty:
        print("✅ Zakazga loyiq tovarlar topilmadi.")
        return

    # --- 7. POCHKA HISOBLASH (Sizning qoidalaringiz) ---
    def dona_to_pochka(dona):
        dona = float(dona)
        if dona <= 2: return 0  # Siz so'ragan qoida
        if dona <= 4: return 1
        if dona <= 10: return 2
        if dona <= 15: return 3
        if dona <= 23: return 4
        if dona <= 29: return 5
        return math.ceil(dona / 6)

    hisobot_final['quantity'] = hisobot_final['kutulyotgan sotuv'].apply(dona_to_pochka).astype(int)
    hisobot_final = hisobot_final[hisobot_final['quantity'] > 0].copy()

    if hisobot_final.empty:
        print("✅ Zakaz soni 0.")
        return

    # --- 8. BAZAGA YOZISH (YANGI REJIM) ---
    hisobot_final['sana_str'] = hisobot_final['max_import_sana'].dt.strftime('%d.%m.%Y')
    hisobot_final['color'] = hisobot_final['Цвет'].fillna('N/A').astype(str) + " (" + hisobot_final['sana_str'] + ")"

    # O'ZGARISH: Existing orders tekshiruvi olib tashlandi.
    # Biz to'g'ridan-to'g'ri hisobot_final dan foydalanamiz.

    rename_map = {
        'Артикул': 'zakaz_id',
        'Поставщик': 'supplier',
        'Категория': 'category',
        'Подкатегория': 'subcategory',
        'Магазин': 'shop',
        'Фото': 'photo',
        'max_import_sana': 'import_date',
        'Hozirgi_Qoldiq': 'hozirgi_qoldiq',
        'Prodano': 'prodano',
        'Дней прошло': 'days_passed',
        'o\'rtcha sotuv': 'ortacha_sotuv',
        'kutulyotgan sotuv': 'kutilyotgan_sotuv',
        'Tovar Holati': 'tovar_holati',
        'supply_price': 'supply_price'
    }

    orders_to_db = hisobot_final.rename(columns=rename_map)
    orders_to_db['artikul'] = orders_to_db['zakaz_id']
    orders_to_db['status'] = 'Kutilmoqda'
    orders_to_db['created_at'] = datetime.now(TASHKENT_TZ).replace(tzinfo=None).date()
    orders_to_db['import_date'] = pd.to_datetime(orders_to_db['import_date']).dt.date

    target_cols = [
        'zakaz_id', 'supplier', 'artikul', 'category', 'subcategory', 'shop', 'color', 'photo',
        'quantity', 'supply_price', 'hozirgi_qoldiq', 'prodano', 'days_passed', 'ortacha_sotuv', 'kutilyotgan_sotuv', 'tovar_holati',
        'import_date', 'created_at', 'status'
    ]
    orders_to_db = orders_to_db[[c for c in target_cols if c in orders_to_db.columns]]

    try:
        with engine.begin() as conn:
            # 1. Eski 'Kutilmoqda' zakazlarni tozalaymiz (Sozlamalar darhol ta'sir qilishi uchun)
            print("🧹 Eski 'Kutilmoqda' zakazlari o'chirilmoqda...")
            conn.execute(text("DELETE FROM generated_orders WHERE status = 'Kutilmoqda'"))

            # 2. Yangi hisoblangan zakazlarni yozamiz
            orders_to_db.to_sql("generated_orders", conn, if_exists="append", index=False)

        print(f"✅ BAZA YANGILANDI: {len(orders_to_db)} ta yangi zakaz yozildi.")

    except Exception as e:
        print(f"❌ Bazaga yozishda xatolik: {e}")

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
