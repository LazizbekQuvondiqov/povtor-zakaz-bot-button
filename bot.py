# --- BU BOT.PY FAYLINING ZAMONAVIY INTERFEYSLI VARIANTI ---
import uuid # <-- Buni importlar qatoriga qo'shing
import asyncio
import pandas as pd
import io
from datetime import datetime

from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
    BufferedInputFile, ReplyKeyboardMarkup, KeyboardButton
)
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command, BaseFilter

from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import config
import db_manager
import data_engine
# Buni esa importlardan keyin, bot=Bot(...) dan oldinroqqa qo'ying
STAT_CACHE = {}
# --- Bot sozlamalari ---
bot = Bot(token=config.TELEGRAM_BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

class Registration(StatesGroup):
    choosing_name = State()
    changing_name = State()
    # Yangi statelar:
    filter_category = State()
    filter_subcategory = State()
class SettingsManagement(StatesGroup):
    waiting_for_new_value = State()
    choosing_setting = State()

class IsAdmin(BaseFilter):
    async def __call__(self, message: Message) -> bool:
        return db_manager.is_admin(message.from_user.id)

# --- MENYU TUGMALARI (ZAMONAVIY) ---

def get_admin_keyboard():
    """Adminlar uchun asosiy menyu"""
    kb = [
        # Mana bu yerda "Statistika" bo'lishi shart 👇
        [KeyboardButton(text="📊 Hisobot"), KeyboardButton(text="📈 Statistika")],
        [KeyboardButton(text="⚙️ Sozlamalar"), KeyboardButton(text="🔄 Majburiy Yangilash")]
    ]
    return ReplyKeyboardMarkup(
        keyboard=kb,
        resize_keyboard=True,
        input_field_placeholder="Admin buyruqlarini tanlang..."
    )

def get_supplier_keyboard():
    """Yetkazib beruvchilar uchun asosiy menyu"""
    kb = [
        [KeyboardButton(text="📦 Zakazlarim"), KeyboardButton(text="📈 Statistika")],
        # MANA BU TUGMANI QO'SHING 👇
        [KeyboardButton(text="📅 Import Tahlili"), KeyboardButton(text="📝 Ismni o'zgartirish")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

# --- Yordamchi Funksiyalar ---

async def get_orders_for_supplier(supplier_name: str) -> pd.DataFrame:
    cleaned_name = supplier_name.replace('\u00A0', ' ').strip()
    def _read_db():
        try:
            query = "SELECT * FROM generated_orders WHERE supplier = %(name)s"
            df = pd.read_sql(query, db_manager.engine, params={"name": cleaned_name})
            return df
        except Exception as e:
            print(f"❌ Bazadan o'qishda xatolik: {e}")
            return pd.DataFrame()
    df = await asyncio.to_thread(_read_db)
    return df

# --- START va MENU Logikasi ---

@dp.message(CommandStart())
async def send_welcome(message: Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id

    # 1. Admin menyusi
    if db_manager.is_admin(user_id):
        await message.answer(
            "👋 Assalomu alaykum, <b>Admin!</b>\n\nQuyidagi menyudan kerakli bo'limni tanlang:",
            reply_markup=get_admin_keyboard()
        )
        return

    # 2. Ro'yxatdan o'tgan Supplier menyusi
    supplier = db_manager.get_supplier_by_id(user_id)
    if supplier:
        await message.answer(
            f"👋 Assalomu alaykum, <b>{supplier.name}</b>!\n\nYangi zakazlarni ko'rish uchun tugmani bosing:",
            reply_markup=get_supplier_keyboard()
        )
        return

    # 3. Taklif qilingan (lekin ro'yxatdan o'tmagan) foydalanuvchi
    invitation = db_manager.check_invitation(user_id)
    if invitation:
        # --- O'ZGARISH SHU YERDA ---
        # To'g'ridan-to'g'ri ro'yxatni chiqarish o'rniga, Kategoriya tanlashga yo'naltiramiz
        categories = db_manager.get_unassigned_categories()

        if not categories:
            await message.answer("Hozircha bo'sh yetkazib beruvchi nomlari yo'q.")
            return

        kb_builder = []
        for cat in categories:
            # Callback datani 'regCat_' deb nomlaymiz (register jarayoni ekanini bilish uchun)
            kb_builder.append([InlineKeyboardButton(text=cat, callback_data=f"regCat_{cat}")])

        keyboard = InlineKeyboardMarkup(inline_keyboard=kb_builder)

        await message.answer(
            "👋 Assalomu alaykum! Tizimga kirish uchun avval faoliyat turingizni (Kategoriya) tanlang:",
            reply_markup=keyboard
        )
        await state.set_state(Registration.filter_category)
        # ---------------------------
    else:
        await message.answer("🚫 Kechirasiz, siz tizimga taklif qilinmagansiz.")
# --- ADMIN TUGMALARI UCHUN HANDLERLAR ---

@dp.message(IsAdmin(), F.text == "⚙️ Sozlamalar")
async def show_settings_text(message: types.Message, state: FSMContext):
    await show_settings_logic(message, state)

@dp.message(IsAdmin(), Command("settings"))
async def show_settings_command(message: types.Message, state: FSMContext):
    await show_settings_logic(message, state)

async def show_settings_logic(message: types.Message, state: FSMContext):
    await state.clear()
    settings = db_manager.get_all_settings()

    text = "<b>⚙️ Tahlil qoidalari:</b>\n\n"
    rules = [
        (f"<b>{i}-Qoida:</b> {int(settings.get(f'm{i}_min_days', 0))}-{int(settings.get(f'm{i}_max_days', 0))} kun, "
         f"{int(settings.get(f'm{i}_percentage', 0))}%+")
        for i in range(1, 5)
    ]
    text += "\n".join(rules)

    buttons = [
        [InlineKeyboardButton(text=f"✏️ {i}-Qoida", callback_data=f"edit_rule_{i}")] for i in range(1, 5)
    ]
    buttons.append([InlineKeyboardButton(text="❌ Yopish", callback_data="cancel_settings")])

    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@dp.message(IsAdmin(), F.text == "🔄 Majburiy Yangilash")
async def force_update_text(message: types.Message):
    await force_update_logic(message)

@dp.message(IsAdmin(), Command("force_update"))
async def force_update_command(message: types.Message):
    await force_update_logic(message)

async def force_update_logic(message: types.Message):
    await message.answer("⏳ <b>Yangilash boshlandi...</b>\nBot ishlashda davom etadi.")
    asyncio.create_task(asyncio.to_thread(data_engine.run_full_update))

@dp.message(IsAdmin(), F.text == "📊 Hisobot")
async def report_text(message: types.Message):
    await report_logic(message)

@dp.message(IsAdmin(), Command("report"))
async def report_command(message: types.Message):
    await report_logic(message)

async def report_logic(message: types.Message):
    await message.answer("⏳ Hisobot tayyorlanmoqda...")
    report_df = await asyncio.to_thread(db_manager.get_full_report_data)

    if report_df.empty:
        await message.answer("⚠️ Ma'lumot yo'q.")
        return

    # --- TUZATISH (VAQTNI TASHKENT VAQTIGA O'TKAZISH) ---
    for col in report_df.select_dtypes(include=['datetimetz', 'datetime']).columns:
        # Agar ustunda vaqt zonasi (timezone) bo'lsa:
        if report_df[col].dt.tz is not None:
            # 1. Avval vaqtni O'zbekiston vaqtiga o'giramiz
            report_df[col] = report_df[col].dt.tz_convert('Asia/Tashkent')
            # 2. Keyin Excel qabul qilishi uchun "timezone" belgisini olib tashlaymiz
            # (Lekin soat o'zgarib ketmaydi, Toshkent vaqtida qoladi)
            report_df[col] = report_df[col].dt.tz_localize(None)
    # --- TUGADI ---

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        report_df.to_excel(writer, index=False, sheet_name='Hisobot')

        # Excel ustunlarini chiroyli qilish (Avtomatik kengaytirish)
        worksheet = writer.sheets['Hisobot']
        for i, col in enumerate(report_df.columns):
            width = max(report_df[col].astype(str).map(len).max(), len(col)) + 2
            worksheet.set_column(i, i, width)

    output.seek(0)

    file = BufferedInputFile(output.getvalue(), filename=f"hisobot_{datetime.now().strftime('%Y-%m-%d')}.xlsx")
    await message.answer_document(file, caption="✅ Hisobot tayyor.")

@dp.message(IsAdmin(), Command("invite"))
async def invite_command(message: types.Message):
    try:
        ids = [int(p) for p in message.text.split()[1:] if p.isdigit()]
        if not ids:
            await message.answer("Format: <code>/invite 12345678</code>")
            return
        added, existed = db_manager.invite_users(ids)
        await message.answer(f"✅ Qo'shildi: {added}\n⚠️ Mavjud: {existed}")
    except Exception as e:
        await message.answer(f"Xato: {e}")
# SHU YERGA TASHLA:
@dp.message(F.text == "📈 Statistika")
async def show_statistics(message: types.Message):
    user_id = message.from_user.id
    
    # 1. ADMIN LOGIKASI (Ichma-ich kirish)
    if db_manager.is_admin(user_id):
        categories = db_manager.get_stat_categories_global()
        
        if not categories:
            await message.answer("✅ Hozircha aktiv zakazlar yo'q.")
            return

        kb = []
        for cat in categories:
            # Callback data: 'stCat_' + kategoriya nomi
            kb.append([InlineKeyboardButton(text=f"📂 {cat}", callback_data=f"stCat_{cat}")])
        
        # Yopish tugmasi
        kb.append([InlineKeyboardButton(text="❌ Yopish", callback_data="del_msg")])
        
        await message.answer(
            "📊 <b>UMUMIY STATISTIKA</b>\n\nQaysi bo'limni ko'rmoqchisiz?",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
        )
        return

    # 2. Agar SUPPLIER bo'lsa -> Ismi va Batafsil statstika
    
    # --- ISMNI ANIQLASH QISMI QO'SHILDI ---
    supplier = db_manager.get_supplier_by_id(user_id)
    current_name = supplier.name if supplier else "Noma'lum"
    # --------------------------------------

    data = db_manager.get_supplier_stats_detailed(user_id)

    if not data:
        await message.answer(f"👤 Ism: <b>{current_name}</b>\n✅ <b>Ajoyib!</b> Hozircha sizda bajarilmagan zakazlar yo'q.")
        return

    # Ma'lumotlarni chiroyli formatlash
    report = {}
    total_packs = 0

    for cat, sub, qty in data:
        if cat not in report:
            report[cat] = []
        report[cat].append(f"▫️ {sub}: <b>{int(qty)} pochka</b>")
        total_packs += qty

    # Xabarni yig'ish (ISMNI QO'SHAMIZ)
    text = f"📊 <b>SIZNING ZAKAZLARINGIZ:</b>\n"
    text += f"👤 Ism: <b>{current_name}</b>\n\n"  # <-- MANA SHU YERGA QO'SHILDI
    
    for category, lines in report.items():
        text += f"📂 <b>{category}</b>\n"
        text += "\n".join(lines) + "\n\n"

    text += f"━━━━━━━━━━━━━━\n🚛 <b>JAMI: {int(total_packs)} POCHKA</b>"

    await message.answer(text)
# -----------------------------------------------------------

# --- SUPPLIER TUGMALARI UCHUN HANDLERLAR ---

@dp.message(F.text == "📦 Zakazlarim")
async def my_orders_text(message: types.Message):
    # Text xabarni CallbackQuery logicaga moslaymiz
    # Bu yerda to'g'ridan-to'g'ri logikani yozamiz
    supplier = db_manager.get_supplier_by_id(message.from_user.id)
    if not supplier:
        await message.answer("❌ Tizimga kirmagansiz. /start")
        return

    msg = await message.answer("⏳ Yuklanmoqda...")
    orders_df = await get_orders_for_supplier(supplier.name)

    if orders_df.empty:
        await msg.edit_text("✅ Yangi zakazlar yo'q.")
        return

    pending = orders_df[orders_df['status'] == 'Kutilmoqda'].copy()
    if pending.empty:
        await msg.edit_text("✅ Javob berilmagan zakazlar yo'q.")
        return

    await msg.delete() # "Yuklanmoqda" ni o'chiramiz

    grouped = pending.groupby('artikul')
    await message.answer(f"📥 <b>{len(grouped)} ta</b> artikul bo'yicha zakaz bor:")

    for article, group in grouped:
        first = group.iloc[0]
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Topdim", callback_data=f"feedback:Topdim:{article}"),
             InlineKeyboardButton(text="❌ Topilmadi", callback_data=f"feedback:Topilmadi:{article}")]
        ])
        price = first.get('supply_price', 0)
        try:
            price_str = f"{float(price):,.0f}".replace(",", " ")
        except:
            price_str = "0"

        caption = f"📦 <b>{article}</b>\n💵 Tan Narx: <b>{price_str} so'm</b>\nToifa: {first.get('subcategory', '-')}\n"

        for shop, s_group in group.groupby('shop'):
            caption += f"\n🏪 <b>{shop}:</b>"
            for _, row in s_group.iterrows():
                caption += f"\n  - {row.get('color','-')}: <b>{row.get('quantity',0)} pochka</b>"

        photo = str(first.get('photo', ''))
        try:
            if photo.startswith('http'):
                if len(caption) > 1024:
                    await bot.send_photo(message.chat.id, photo)
                    await bot.send_message(message.chat.id, caption, reply_markup=keyboard)
                else:
                    await bot.send_photo(message.chat.id, photo, caption=caption, reply_markup=keyboard)
            else:
                await bot.send_message(message.chat.id, caption, reply_markup=keyboard)
        except Exception:
            await bot.send_message(message.chat.id, caption, reply_markup=keyboard)
        await asyncio.sleep(0.3)

@dp.message(F.text == "📝 Ismni o'zgartirish")
async def change_name_text(message: types.Message, state: FSMContext):
    # 1-qadam: Mavjud bo'sh kategoriyalarni olish
    categories = db_manager.get_unassigned_categories()

    if not categories:
        await message.answer("⚠️ Hozircha bo'sh yetkazib beruvchilar yoki zakazlar yo'q.")
        return

    # Kategoriyalarni tugma qilish
    # Callback data sig'ishi uchun qisqartma ishlatamiz yoki shundayligicha (agar nomlar uzun bo'lmasa)
    kb_builder = []
    for cat in categories:
        kb_builder.append([InlineKeyboardButton(text=cat, callback_data=f"catSel_{cat}")])

    keyboard = InlineKeyboardMarkup(inline_keyboard=kb_builder)

    await message.answer("📂 Iltimos, faoliyatingiz turini (Kategoriya) tanlang:", reply_markup=keyboard)
    await state.set_state(Registration.filter_category)
# --- ESKI CALLBACK VA SOZLAMALAR LOGIKASI ---

@dp.callback_query(Registration.choosing_name, F.data.startswith("register_"))
async def process_register(callback: CallbackQuery, state: FSMContext):
    name = callback.data.split("_", 1)[1]
    if db_manager.register_supplier(callback.from_user.id, name):
        await callback.message.delete()
        await callback.message.answer(f"✅ Xush kelibsiz, <b>{name}</b>!", reply_markup=get_supplier_keyboard())
    else:
        await callback.message.edit_text("❌ Bu nom band.")
    await state.clear()

@dp.callback_query(Registration.changing_name, F.data.startswith("change_"))
async def process_change(callback: CallbackQuery, state: FSMContext):
    new_name = callback.data.split("_", 1)[1]
    success, old_name = db_manager.update_supplier_name(callback.from_user.id, new_name)
    if success:
        await callback.message.delete()
        await callback.message.answer(
            f"🔄 Ism o'zgardi:\nEski: {old_name}\nYangi: <b>{new_name}</b>",
            reply_markup=get_supplier_keyboard()
        )
    else:
        await callback.message.edit_text("❌ Xatolik.")
    await state.clear()

@dp.callback_query(F.data.startswith("feedback:"))
async def feedback_handler(callback: CallbackQuery):
    _, status, zakaz_id = callback.data.split(":")
    
    # 1. Bazada statusni yangilaymiz
    if db_manager.update_order_status(zakaz_id, status):
        
        # 2. Xabarni o'zgartirish (eski logika)
        icon = "✅" if status == "Topdim" else "❌"
        # Caption yoki Text borligini tekshiramiz
        old_caption = callback.message.caption or callback.message.text or ""
        
        # Agar oldin javob yozilmagan bo'lsa, javobni qo'shamiz
        if "Javob:" not in old_caption:
            new_txt = old_caption + f"\n\n<b>Javob: {icon} {status}</b>"
        else:
            new_txt = old_caption # Qayta bosilganda matn buzilmasligi uchun
            
        try:
            if callback.message.photo:
                await callback.message.edit_caption(caption=new_txt, reply_markup=None)
            else:
                await callback.message.edit_text(new_txt, reply_markup=None)
        except TelegramBadRequest:
            await callback.message.edit_reply_markup(reply_markup=None)

        # --- YANGI QO'SHILGAN QISM: KANALGA YUBORISH ---
        if status == "Topdim":
            try:
                # Kim tasdiqlaganini bilish uchun (ixtiyoriy)
                user_info = f"\n\n👤 <b>Tasdiqladi:</b> {callback.from_user.full_name}"
                channel_caption = old_caption + user_info

                if callback.message.photo:
                    # Rasmli xabar bo'lsa
                    photo_id = callback.message.photo[-1].file_id
                    await bot.send_photo(chat_id=config.ARCHIVE_CHANNEL_ID, photo=photo_id, caption=channel_caption)
                else:
                    # Faqat matnli xabar bo'lsa
                    await bot.send_message(chat_id=config.ARCHIVE_CHANNEL_ID, text=channel_caption)
            except Exception as e:
                print(f"❌ Kanalga yuborishda xatolik: {e}")
        # -----------------------------------------------

    else:
        await callback.answer("❌ Xatolik yoki bu zakaz allaqachon o'zgargan", show_alert=True)
# --- SOZLAMALAR CALLBACKLARI ---
@dp.callback_query(F.data.startswith("edit_rule_"))
async def edit_rule(callback: CallbackQuery, state: FSMContext):
    rule = callback.data.split("_")[-1]
    parts = {f"m{rule}_min_days": "Min kun", f"m{rule}_max_days": "Max kun", f"m{rule}_percentage": "Foiz %"}
    btns = [[InlineKeyboardButton(text=v, callback_data=f"edit_setting_{k}")] for k, v in parts.items()]
    btns.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="back_to_rules")])
    await callback.message.edit_text(f"<b>{rule}-Qoida</b>ni tahrirlash:", reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await state.set_state(SettingsManagement.choosing_setting)

@dp.callback_query(SettingsManagement.choosing_setting, F.data == "back_to_rules")
async def back_rules(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_settings_logic(callback.message, state) # Logic funksiyani chaqiramiz

@dp.callback_query(SettingsManagement.choosing_setting, F.data.startswith("edit_setting_"))
async def edit_val(callback: CallbackQuery, state: FSMContext):
    name = callback.data.replace("edit_setting_", "")
    await state.update_data(setting_to_edit=name)
    await callback.message.edit_text(f"<code>{name}</code> uchun yangi qiymat yozing:", reply_markup=None)
    await state.set_state(SettingsManagement.waiting_for_new_value)

@dp.message(SettingsManagement.waiting_for_new_value)
async def save_val(message: Message, state: FSMContext):
    if not message.text.replace('.', '', 1).isdigit():
        await message.answer("❌ Raqam yozing.")
        return
    data = await state.get_data()
    if db_manager.update_setting(data['setting_to_edit'], float(message.text)):
        await message.answer("✅ Saqlandi.")
    else:
        await message.answer("❌ Xatolik.")
    await state.clear()
    await show_settings_logic(message, state)

@dp.callback_query(F.data == "cancel_settings")
async def cancel_s(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()



@dp.callback_query(Registration.filter_category, F.data.contains("Cat_") | F.data.contains("catSel_"))
async def category_selected(callback: CallbackQuery, state: FSMContext):
    # Datadan kategoriyani ajratib olamiz (prefiksni olib tashlab)
    data_str = callback.data
    if "regCat_" in data_str: category = data_str.split("regCat_", 1)[1]
    elif "catSel_" in data_str: category = data_str.split("catSel_", 1)[1]
    else: category = data_str # Ehtiyot shart

    await state.update_data(selected_category=category)

    subcategories = db_manager.get_unassigned_subcategories(category)

    if not subcategories:
        await callback.message.edit_text("⚠️ Bu kategoriyada podkategoriyalar topilmadi.")
        return

    kb_builder = []
    for sub in subcategories:
        # Podkategoriya tanlanganda ham universal prefiks
        kb_builder.append([InlineKeyboardButton(text=sub, callback_data=f"uniSub_{sub}")])

    kb_builder.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="back_to_cats_uni")])

    await callback.message.edit_text(
        f"📂 <b>{category}</b> tanlandi.\nEndi aniq turini (Podkategoriya) tanlang:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_builder)
    )
    await state.set_state(Registration.filter_subcategory)

# 2. Podkategoriya tanlanganda -> Supplierlar chiqadi
@dp.callback_query(Registration.filter_subcategory, F.data.startswith("uniSub_"))
async def subcategory_selected(callback: CallbackQuery, state: FSMContext):
    subcategory = callback.data.split("uniSub_", 1)[1]
    data = await state.get_data()
    category = data.get("selected_category")

    suppliers = db_manager.get_unassigned_suppliers_by_filter(category, subcategory)

    if not suppliers:
        await callback.message.edit_text("⚠️ Afsuski, bu bo'limda bo'sh nomlar qolmadi.")
        return

    # Hozir foydalanuvchi ro'yxatdan o'tyaptimi yoki ism o'zgartiryaptimi?
    # Buni bilish uchun check_invitation yoki state holatidan foydalanamiz.
    # Lekin oddiyroq yo'li: Tugma bosilganda bazada supplier bormi yo'qmi tekshiramiz.

    kb_builder = []
    user_id = callback.from_user.id
    is_registered = db_manager.get_supplier_by_id(user_id) is not None

    for name in suppliers:
        if is_registered:
            # Ism o'zgartirish rejimi
            action = f"change_{name}"
        else:
            # Ro'yxatdan o'tish rejimi
            action = f"register_{name}"

        kb_builder.append([InlineKeyboardButton(text=name, callback_data=action)])

    kb_builder.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="back_to_subs_uni")])

    await callback.message.edit_text(
        f"✅ <b>{subcategory}</b> bo'yicha bo'sh nomlar:\nO'zingiznikini tanlang:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_builder)
    )
    # Bu yerda stateni o'zgartirish shart emas, callbacklar (register_ yoki change_) o'zi hal qiladi
    # Lekin to'g'ri handler ushlab olishi uchun:
    if is_registered:
        await state.set_state(Registration.changing_name)
    else:
        await state.set_state(Registration.choosing_name)

# Orqaga qaytish logikasi
@dp.callback_query(F.data == "back_to_cats_uni")
async def back_uni_cat(callback: CallbackQuery, state: FSMContext):
    # Qayta start bergandek bo'lamiz (Admin yoki Userligiga qarab)
    await send_welcome(callback.message, state)

@dp.callback_query(F.data == "back_to_subs_uni")
async def back_uni_sub(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    category = data.get("selected_category")

    # Obyektni o'zgartirish o'rniga, uning nusxasini yaratib, ma'lumotni o'zgartiramiz
    new_callback = callback.model_copy(update={'data': f"regCat_{category}"})

    await category_selected(new_callback, state)
# 2. Podkategoriya tanlanganda -> Supplierlar chiqadi
@dp.callback_query(Registration.filter_subcategory, F.data.startswith("subSel_"))
async def subcategory_selected(callback: CallbackQuery, state: FSMContext):
    subcategory = callback.data.split("_", 1)[1]
    data = await state.get_data()
    category = data.get("selected_category")

    suppliers = db_manager.get_unassigned_suppliers_by_filter(category, subcategory)

    if not suppliers:
        await callback.message.edit_text("⚠️ Afsuski, bu bo'limda bo'sh nomlar qolmadi.")
        return

    kb_builder = []
    for name in suppliers:
        # Bu yerda eski 'change_' prefiksini ishlatamiz, chunki oxirgi logika o'zgarmasin
        kb_builder.append([InlineKeyboardButton(text=name, callback_data=f"change_{name}")])

    kb_builder.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="back_to_subs")])

    await callback.message.edit_text(
        f"✅ <b>{subcategory}</b> bo'yicha bo'sh nomlar:\nO'zingiznikini tanlang:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_builder)
    )
    # Bu yerdan buyog'iga eski logika (changing_name) ishlashni davom etadi
    await state.set_state(Registration.changing_name)

# --- "ORQAGA" TUGMALARI UCHUN HANDLERLAR ---

@dp.callback_query(F.data == "back_to_cats")
async def back_to_categories(callback: CallbackQuery, state: FSMContext):
    await change_name_text(callback.message, state)

@dp.callback_query(F.data == "back_to_subs")
async def back_to_subcategories(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    category = data.get("selected_category")

    # To'g'ri usul:
    new_callback = callback.model_copy(update={'data': f"catSel_{category}"})

    await category_selected(new_callback, state)
# --- MAIN LOOP ---
async def scheduled_update_job():
    print("⏰ Avto-yangilash...")
    await asyncio.to_thread(data_engine.run_full_update)

async def send_reminders():
    # Eslatma logikasi o'zgarishsiz qoladi, faqat asinxronlikka e'tibor bering
    pending = db_manager.get_pending_orders_for_reminder(24)
    if not pending: return
    reminders = {}
    for o in pending:
        reminders.setdefault(o['telegram_id'], []).append(f"- {o['subcategory']} ({o['artikul']})")

    for uid, items in reminders.items():
        try:
            await bot.send_message(uid, "<b>🔔 Eslatma!</b> Javob berilmagan zakazlar:\n" + "\n".join(items))
        except Exception: pass


# --- ADMIN STATISTIKA NAVIGATSIYASI ---

# 1. Kategoriya tanlanganda -> Podkategoriyalar chiqadi
@dp.callback_query(F.data.startswith("stCat_"))
async def stat_category_click(callback: CallbackQuery):
    # Kategoriya nomini olamiz
    category = callback.data.split("stCat_", 1)[1]
    
    subs = db_manager.get_stat_subcategories_global(category)
    
    kb = []
    for sub in subs:
        # --- MUHIM O'ZGARISH ---
        # Uzun nomlarni sig'dirish uchun unikal ID ishlatamiz
        unique_id = str(uuid.uuid4())[:8]  # Masalan: 'a1b2c3d4'
        
        # Ma'lumotni xotiraga saqlaymiz: ID -> (Kategoriya, Podkategoriya)
        STAT_CACHE[unique_id] = (category, sub)
        
        # Tugmaga faqat qisqa ID ni yozamiz (Xatolik bermaydi)
        kb.append([InlineKeyboardButton(text=f"🔹 {sub}", callback_data=f"stSub_{unique_id}")])
    
    # Orqaga qaytish
    kb.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="stBack_root")])
    
    await callback.message.edit_text(
        f"📂 <b>{category}</b>\nIchki turlarni tanlang:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )

# 2. Podkategoriya tanlanganda -> Aniq son chiqadi
@dp.callback_query(F.data.startswith("stSub_"))
async def stat_subcategory_click(callback: CallbackQuery):
    # Qisqa ID ni olamiz
    unique_id = callback.data.split("stSub_", 1)[1]
    
    # Xotiradan haqiqiy nomlarni qidiramiz
    data = STAT_CACHE.get(unique_id)
    
    if not data:
        await callback.answer("⚠️ Ma'lumot eskirgan, qaytadan oching.", show_alert=True)
        return

    category, subcategory = data
    
    total_packs = db_manager.get_stat_total_packs(category, subcategory)
    
    # Qayta tanlash uchun tugma
    kb = [
        [InlineKeyboardButton(text="⬅️ Ortga qaytish", callback_data=f"stCat_{category}")]
    ]
    
    await callback.message.edit_text(
        f"📊 <b>NATIJA:</b>\n\n"
        f"📂 Kategoriya: <b>{category}</b>\n"
        f"🔹 Podkategoriya: <b>{subcategory}</b>\n\n"
        f"📦 Jami zakaz: <b>{int(total_packs)} pochka</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )

# 3. "Orqaga" va "Yopish" tugmalari
@dp.callback_query(F.data == "stBack_root")
async def stat_back_root(callback: CallbackQuery):
    # Qaytadan kategoriyalarni yuklaymiz
    categories = db_manager.get_stat_categories_global()
    kb = []
    for cat in categories:
        kb.append([InlineKeyboardButton(text=f"📂 {cat}", callback_data=f"stCat_{cat}")])
    kb.append([InlineKeyboardButton(text="❌ Yopish", callback_data="del_msg")])
    
    await callback.message.edit_text(
        "📊 <b>UMUMIY STATISTIKA</b>\n\nQaysi bo'limni ko'rmoqchisiz?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )

@dp.callback_query(F.data == "del_msg")
async def delete_msg(callback: CallbackQuery):
    await callback.message.delete()

#vaqtinchalik buzish uchun kod funksiyasi 
# --- 1. SHU FUNKSIYANI TASHLA ---
async def check_access(handler, event, data):
    user_id = event.from_user.id
    
    # Ruxsat berilgan IDlar ro'yxati (Ikkalasini shu yerga yozing)
    allowed_ids = [8081878196, 1205534758]

    # Agar foydalanuvchi shu ro'yxatda BO'LMASA -> Xato beramiz
    if user_id not in allowed_ids:
        try: await event.answer("⚠️ Botda texnik xatolik ro'y berdi Aniqlanmagan xato", show_alert=True)
        except: await event.answer("⚠️ Botda texnik xatolik ro'y berdi Aniqlanmagan xato")
        return
        
    # Ro'yxatda bor bo'lsa -> Ruxsat
    return await handler(event, data)
# shu yergacha 



async def main():
    db_manager.init_db()
    await bot.delete_webhook(drop_pending_updates=True)
    scheduler = AsyncIOScheduler(timezone="Asia/Tashkent")
    scheduler.add_job(scheduled_update_job, 'cron', hour=3, minute=0)
    scheduler.add_job(send_reminders, 'cron', hour=10, minute=0)
    scheduler.start()

    
    # vaqtinchalik buzish
    dp.message.outer_middleware(check_access)
    dp.callback_query.outer_middleware(check_access)
    # shu yergacha 

    
    await dp.start_polling(bot)






# -------------------------------------------------------------------------
# --- IMPORT (KUN) TAHLILI LOGIKASI (GLOBAL KO'RISH) ---
# -------------------------------------------------------------------------

@dp.message(F.text == "📅 Import Tahlili")
async def import_analysis_start(message: types.Message):
    # 1. Bazadan Admin o'rnatgan qoidalarni olamiz
    settings = db_manager.get_all_settings()
    ranges = []
    
    rule_labels = {
        4: "🔥 4-Qoida (Eng yangi)",
        3: "⚡️ 3-Qoida",
        2: "⚠️ 2-Qoida",
        1: "❄️ 1-Qoida (Eski)"
    }

    # 4 dan 1 gacha aylanamiz
    for i in [4, 3, 2, 1]:
        min_d = int(settings.get(f'm{i}_min_days', 0))
        max_d = int(settings.get(f'm{i}_max_days', 0))
        if max_d > 0:
            btn_text = f"{rule_labels[i]}: {min_d}-{max_d} kun"
            ranges.append((min_d, max_d, btn_text))
    
    kb = []
    for mn, mx, label in ranges:
        kb.append([InlineKeyboardButton(text=label, callback_data=f"impRange_{mn}-{mx}")])
    
    kb.append([InlineKeyboardButton(text="❌ Yopish", callback_data="del_msg")])
    
    await message.answer(
        "📅 <b>IMPORT TAHLILI</b>\n\n"
        "Bozordagi umumiy holatni ko'rish uchun muddatni tanlang:", 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )

# 1. Kun tanlanganda -> Kategoriya chiqadi
@dp.callback_query(F.data.startswith("impRange_"))
async def imp_range_click(callback: CallbackQuery):
    mn, mx = map(int, callback.data.split("_")[1].split("-"))
    
    # GLOBAL qidiruv (Supplier filtrlanmaydi)
    cats = db_manager.get_stats_by_import_days(mn, mx)
    
    if not cats:
        await callback.answer("⚠️ Bu muddatda zakazlar yo'q", show_alert=True)
        return

    kb = []
    for cat in cats:
        unique_id = str(uuid.uuid4())[:8]
        STAT_CACHE[unique_id] = (mn, mx, cat)
        kb.append([InlineKeyboardButton(text=f"📂 {cat}", callback_data=f"impCat_{unique_id}")])
    
    kb.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="impBack_root")])
    
    await callback.message.edit_text(
        f"📅 <b>{mn}-{mx} kunlik tovarlar</b>\nKategoriyani tanlang:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )

# 2. Kategoriya tanlanganda -> Podkategoriya chiqadi
@dp.callback_query(F.data.startswith("impCat_"))
async def imp_cat_click(callback: CallbackQuery):
    uid = callback.data.split("_")[1]
    data = STAT_CACHE.get(uid)
    if not data: return
    
    mn, mx, cat = data
    subs = db_manager.get_stats_by_import_days(mn, mx, category=cat)
    
    kb = []
    for sub in subs:
        unique_id = str(uuid.uuid4())[:8]
        STAT_CACHE[unique_id] = (mn, mx, cat, sub)
        kb.append([InlineKeyboardButton(text=f"🔹 {sub}", callback_data=f"impSub_{unique_id}")])
    
    kb.append([InlineKeyboardButton(text="⬅️ Boshiga", callback_data="impBack_root")])
    
    await callback.message.edit_text(
        f"📂 <b>{cat}</b> ({mn}-{mx} kun)\nPodkategoriyani tanlang:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )

# 3. Podkategoriya tanlanganda -> KARTOCHKALAR CHIQADI
# --- BOT.PY ---

@dp.callback_query(F.data.startswith("impSub_"))
async def imp_sub_click(callback: CallbackQuery):
    uid = callback.data.split("_")[1]
    data = STAT_CACHE.get(uid)
    if not data: return
    
    mn, mx, cat, sub = data
    
    # 1. Bazadan ma'lumot olamiz
    orders_df = await asyncio.to_thread(db_manager.get_import_orders_detailed, mn, mx, cat, sub)
    
    if orders_df.empty:
        await callback.answer("⚠️ Ma'lumot topilmadi.", show_alert=True)
        return

    await callback.message.delete()
    await callback.message.answer(f"⏳ <b>{cat} > {sub}</b> ({mn}-{mx} kun)\nMa'lumotlar yuklanmoqda...")

    # 2. Artikul bo'yicha guruhlaymiz
    grouped = orders_df.groupby('artikul')
    
    for article, group in grouped:
        first = group.iloc[0]
        price = first.get('supply_price', 0)
        try:
            price_str = f"{float(price):,.0f}".replace(",", " ")
        except:
            price_str = "0"

        # --- O'ZGARISH: TUGMALAR QO'SHILDI ---
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Topdim", callback_data=f"feedback:Topdim:{article}"),
             InlineKeyboardButton(text="❌ Topilmadi", callback_data=f"feedback:Topilmadi:{article}")]
        ])
        # -------------------------------------

        # Matn: Supplier nomi va Umumiy ma'lumot
        caption = f"📦 <b>{article}</b>\n"
        caption += f"👤 Postavchik: <b>{first.get('supplier', 'Noma\'lum')}</b>\n"
        caption += f"💵 Tan Narx: <b>{price_str} so'm</b>\n"
        caption += f"Toifa: {first.get('subcategory', '-')}\n"

        # Do'konlar
        for shop, s_group in group.groupby('shop'):
            caption += f"\n🏪 <b>{shop}:</b>"
            for _, row in s_group.iterrows():
                color_info = row.get('color', '-')
                caption += f"\n  - {color_info}: <b>{int(row.get('quantity', 0))} pochka</b>"

        photo = str(first.get('photo', ''))
        
        try:
            if photo.startswith('http'):
                if len(caption) > 1024:
                    await bot.send_photo(callback.message.chat.id, photo)
                    # Matn alohida ketganda tugmani matnga ulaymiz
                    await bot.send_message(callback.message.chat.id, caption, reply_markup=keyboard)
                else:
                    # Rasm bilan birga tugma
                    await bot.send_photo(callback.message.chat.id, photo, caption=caption, reply_markup=keyboard)
            else:
                # Faqat matn bo'lsa
                await bot.send_message(callback.message.chat.id, caption, reply_markup=keyboard)
        except Exception:
            await bot.send_message(callback.message.chat.id, caption, reply_markup=keyboard)
        
        await asyncio.sleep(0.3)

    # Ro'yxat tugagach chiqadigan menyu
    kb = [[InlineKeyboardButton(text="🔄 Boshqa bo'lim", callback_data="impBack_root")]]
    await bot.send_message(callback.message.chat.id, "✅ Ro'yxat tugadi.", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
@dp.callback_query(F.data == "impBack_root")
async def imp_back_root(callback: CallbackQuery):
    await import_analysis_start(callback.message)


if __name__ == "__main__":
    asyncio.run(main())
