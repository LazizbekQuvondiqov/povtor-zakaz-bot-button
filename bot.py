# --- BU BOT.PY FAYLINING ZAMONAVIY INTERFEYSLI VARIANTI ---

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
        [KeyboardButton(text="📊 Hisobot"), KeyboardButton(text="⚙️ Sozlamalar")],
        [KeyboardButton(text="🔄 Majburiy Yangilash")]
    ]
    return ReplyKeyboardMarkup(
        keyboard=kb,
        resize_keyboard=True,
        input_field_placeholder="Admin buyruqlarini tanlang..."
    )

def get_supplier_keyboard():
    """Yetkazib beruvchilar uchun asosiy menyu"""
    kb = [
        [KeyboardButton(text="📦 Zakazlarim")],
        [KeyboardButton(text="📝 Ismni o'zgartirish")]
    ]
    return ReplyKeyboardMarkup(
        keyboard=kb,
        resize_keyboard=True,
        input_field_placeholder="Menyudan tanlang..."
    )

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

        caption = f"📦 <b>{article}</b>\n💵 Narx: <b>{price_str} so'm</b>\nToifa: {first.get('subcategory', '-')}\n"

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
    if db_manager.update_order_status(zakaz_id, status):
        icon = "✅" if status == "Topdim" else "❌"
        txt = callback.message.caption or callback.message.text
        new_txt = txt + f"\n\n<b>Javob: {icon} {status}</b>"
        try:
            if callback.message.photo:
                await callback.message.edit_caption(caption=new_txt, reply_markup=None)
            else:
                await callback.message.edit_text(new_txt, reply_markup=None)
        except TelegramBadRequest:
            await callback.message.edit_reply_markup(reply_markup=None)
    else:
        await callback.answer("❌ Xatolik", show_alert=True)

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
    # category_selected funksiyasini chaqiramiz (soxta callback bilan)
    callback.data = f"regCat_{category}"
    await category_selected(callback, state)
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
    # Qayta chaqirish uchun soxta callback yasaymiz
    callback.data = f"catSel_{category}"
    await category_selected(callback, state)


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

async def main():
    db_manager.init_db()
    await bot.delete_webhook(drop_pending_updates=True)
    scheduler = AsyncIOScheduler(timezone="Asia/Tashkent")
    scheduler.add_job(scheduled_update_job, 'cron', hour=3, minute=0)
    scheduler.add_job(send_reminders, 'cron', hour=10, minute=0)
    scheduler.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
