import os
import re
import asyncio
import json
from collections import defaultdict
from datetime import datetime
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.utils.keyboard import ReplyKeyboardMarkup, KeyboardButton

# Загрузка переменных окружения
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")

# Инициализация бота с указанием порта для Render
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# Хранилище данных
storage_data = {
    "deposits": defaultdict(lambda: defaultdict(int)),
    "withdrawals": defaultdict(lambda: defaultdict(int)),
    "player_names": {},
    "all_players": set(),
    "processed_logs": set()
}

def save_data():
    """Сохраняет данные в файл"""
    with open('storage_data.json', 'w') as f:
        json.dump({
            "deposits": {k: dict(v) for k, v in storage_data["deposits"].items()},
            "withdrawals": {k: dict(v) for k, v in storage_data["withdrawals"].items()},
            "player_names": storage_data["player_names"],
            "all_players": list(storage_data["all_players"]),
            "processed_logs": list(storage_data["processed_logs"])
        }, f)

def load_data():
    """Загружает данные из файла"""
    try:
        with open('storage_data.json', 'r') as f:
            data = json.load(f)
            storage_data["deposits"] = defaultdict(lambda: defaultdict(int), 
                {k: defaultdict(int, v) for k, v in data["deposits"].items()})
            storage_data["withdrawals"] = defaultdict(lambda: defaultdict(int), 
                {k: defaultdict(int, v) for k, v in data["withdrawals"].items()})
            storage_data["player_names"] = data["player_names"]
            storage_data["all_players"] = set(data["all_players"])
            storage_data["processed_logs"] = set(data["processed_logs"])
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        pass

# Загружаем данные при старте
load_data()

def extract_datetime(log_line: str) -> datetime:
    """Извлекает дату и время из строки лога"""
    match = re.search(r'\[🎒 (\d{2}\.\d{2} \d{2}:\d{2}:\d{2})\]', log_line)
    if match:
        current_year = datetime.now().year
        date_with_year = f"{match.group(1)}.{current_year}"
        try:
            return datetime.strptime(date_with_year, '%d.%m %H:%M:%S.%Y')
        except ValueError:
            return datetime.strptime(f"28.02 {match.group(1).split(' ')[1]}.{current_year}", 
                                  '%d.%m %H:%M:%S.%Y')
    return None

def get_log_hash(log_line: str) -> str:
    """Создает уникальный хэш для строки лога"""
    dt = extract_datetime(log_line)
    if dt:
        return dt.strftime('%Y%m%d%H%M%S') + ":" + re.sub(r'\[🎒 \d{2}\.\d{2} \d{2}:\d{2}:\d{2}\]', '', log_line).strip()
    return log_line.strip()

def extract_player_info(line: str) -> tuple:
    """Извлекает полный никнейм и короткое имя игрока"""
    line = line.strip()
    
    # Вариант для формата [Клан]ЭмодзиИмя
    match = re.search(r'(\[[^\]]+\])([^\[]+)', line)
    if match:
        clan_tag = match.group(1)
        rest = match.group(2)
        name_match = re.search(r'([^\w]*)([\w]+)', rest)
        if name_match:
            emoji = name_match.group(1).strip()
            name = name_match.group(2)
            full_name = f"{clan_tag}{emoji}{name}"
            short_name = f"{emoji}{name}" if emoji else name
            return full_name, short_name
    
    # Вариант для формата ЭмодзиИмя
    name_match = re.search(r'([^\w]*)(\w+)', line)
    if name_match:
        emoji = name_match.group(1).strip()
        name = name_match.group(2)
        return f"{emoji}{name}", f"{emoji}{name}"
    
    return "Неизвестный", "Неизвестный"

async def main_menu_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Баланс предметов")],
            [KeyboardButton(text="📜 Последние записи")]
        ],
        resize_keyboard=True
    )

async def players_keyboard():
    buttons = [KeyboardButton(text=short_name) 
               for short_name in storage_data["player_names"].keys()]
    return ReplyKeyboardMarkup(
        keyboard=[buttons[i:i+2] for i in range(0, len(buttons), 2)],
        resize_keyboard=True
    )

@dp.message(Command("start"))
async def start(message: types.Message):
    await message.answer(
        "🔍 Бот для учёта баланса предметов\n"
        "Отправьте лог действий",
        reply_markup=await main_menu_keyboard()
    )

@dp.message(Command("history"))
async def show_history(message: types.Message):
    history = sorted(storage_data["processed_logs"], reverse=True)[:10]
    await message.answer("Последние 10 записей:\n" + "\n".join(history))

@dp.message(lambda m: m.text == "📊 Баланс предметов")
async def show_players(message: types.Message):
    if not storage_data["player_names"]:
        await message.answer("Нет данных об игроках", reply_markup=await main_menu_keyboard())
        return
    
    last_log_time = "неизвестно"
    if storage_data["processed_logs"]:
        last_hash = sorted(storage_data["processed_logs"], reverse=True)[0]
        last_log_time = last_hash.split(":")[0]
    
    await message.answer(
        f"Выберите игрока (актуально на {last_log_time}):",
        reply_markup=await players_keyboard()
    )

@dp.message(lambda m: m.text == "📜 Последние записи")
async def show_recent_logs(message: types.Message):
    await show_history(message)

async def show_player_balance(message: types.Message):
    short_name = message.text
    full_name = storage_data["player_names"].get(short_name)
    
    if not full_name:
        await message.answer("Игрок не найден", reply_markup=await main_menu_keyboard())
        return
    
    deposits = storage_data["deposits"].get(full_name, {})
    withdrawals = storage_data["withdrawals"].get(full_name, {})
    
    report = [f"📊 Баланс игрока {full_name}"]
    
    debt_items = []
    for item in sorted(withdrawals.keys()):
        balance = deposits.get(item, 0) - withdrawals.get(item, 0)
        if balance < 0:
            debt_items.append(f"  - {item}: {abs(balance)} шт. (выдано)")
    
    credit_items = []
    for item in sorted(deposits.keys()):
        balance = deposits.get(item, 0) - withdrawals.get(item, 0)
        if balance > 0:
            credit_items.append(f"  - {item}: {balance} шт. (внес)")
    
    if debt_items:
        report.append("\n🔴 Получил:")
        report.extend(debt_items)
    
    if credit_items:
        report.append("\n🟢 Внес:")
        report.extend(credit_items)
    
    total_deposit = sum(deposits.values())
    total_withdrawal = sum(withdrawals.values())
    total_balance = total_deposit - total_withdrawal
    
    report.append("\n📌 Итого:")
    report.append(f"  - Внесено: {total_deposit} шт.")
    report.append(f"  - Получено: {total_withdrawal} шт.")
    report.append(f"  - Общий баланс: {total_balance} шт. {'(получил)' if total_balance < 0 else '(внес)' if total_balance > 0 else ''}")
    
    await message.answer("\n".join(report), reply_markup=await main_menu_keyboard())

async def parse_log(message: types.Message):
    new_logs_count = 0

    for line in message.text.split('\n'):
        if not line.strip() or '📰 Журнал Действий' in line:
            continue

        log_hash = get_log_hash(line)
        if log_hash in storage_data["processed_logs"]:
            continue

        try:
            clean_line = re.sub(r'^\[\🎒\s\d{2}\.\d{2}\s\d{2}:\d{2}:\d{2}\]\s*', '', line)

            if 'отправил в хранилище' in clean_line:
                sender_part = clean_line.split('отправил в хранилище')[0].strip()
                item_part = clean_line.split('отправил в хранилище')[1].strip()
                
                full_name, short_name = extract_player_info(sender_part)
                if full_name != "Неизвестный":
                    item_match = re.search(r'🎒([^\[\]]+?)(?:\[\w+\])?\s*(\d+)\s*шт', item_part)
                    if item_match:
                        item = item_match.group(1).strip()
                        count = int(item_match.group(2))
                        storage_data["deposits"][full_name][item] += count
                        storage_data["all_players"].add(full_name)
                        storage_data["player_names"][short_name] = full_name

            elif 'отправил из хранилища' in clean_line:
                recipient_part = clean_line.split('отправил из хранилища')[1].split('🎒')[0].strip()
                item_part = '🎒' + clean_line.split('🎒')[1] if '🎒' in clean_line else ''
                
                full_name, short_name = extract_player_info(recipient_part)
                if full_name != "Неизвестный":
                    item_match = re.search(r'🎒([^\[\]]+?)(?:\[\w+\])?\s*(\d+)\s*шт', item_part)
                    if item_match:
                        item = item_match.group(1).strip()
                        count = int(item_match.group(2))
                        storage_data["withdrawals"][full_name][item] += count
                        storage_data["all_players"].add(full_name)
                        storage_data["player_names"][short_name] = full_name

            storage_data["processed_logs"].add(log_hash)
            new_logs_count += 1

        except Exception as e:
            print(f"Ошибка обработки: {line}\n{str(e)}")

    save_data()
    
    response = [
        "✅ Лог успешно обработан",
        f"• Обработано строк: {new_logs_count}",
        f"• Всего записей: {len(storage_data['processed_logs'])}",
        f"• Уникальных игроков: {len(storage_data['all_players'])}"
    ]
    
    await message.answer("\n".join(response), reply_markup=await main_menu_keyboard())

@dp.message()
async def handle_message(message: types.Message):
    if message.text in storage_data["player_names"]:
        await show_player_balance(message)
    elif message.text.startswith("📰 Журнал Действий") or "[🎒" in message.text:
        await parse_log(message)
    else:
        await message.answer("Неизвестная команда", reply_markup=await main_menu_keyboard())

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())