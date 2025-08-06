import asyncio
import logging
import os
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import aiosqlite
import aiofiles
from aiogram import Bot, Dispatcher, Router, types
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import Command
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()
API_TOKEN = os.getenv('API_TOKEN')
DEFAULT_TIMEZONE = os.getenv('TIMEZONE', 'UTC')
DAY_START_HOUR = int(os.getenv('DAY_START_HOUR', 0))

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.getLogger('aiosqlite').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)

# Инициализация бота и диспетчера
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(client=bot, fsm_storage=storage)
router = Router()

# Глобальный кэш категорий и ID
CATEGORIES = {'expense': [], 'income': []}
ID_MAPPING_CACHE = {}

# Текст инструкции
INSTRUCTION_TEXT = """### Инструкция по использованию Telegram-бота для учета расходов и доходов

Этот бот позволяет вести учет ваших расходов и доходов, управлять категориями, просматривать статистику, экспортировать данные и настраивать часовой пояс. Вот подробное руководство по всем кнопкам и функциям.

---

### Основные команды и кнопки

1. **/start**  
   - **Описание**: Запускает бота и отображает кнопку "Меню".  
   - **Как использовать**: Введите `/start` в чате с ботом.  
   - **Результат**: Появится приветственное сообщение и кнопка "Меню" для перехода к основным функциям.

2. **Кнопка "Меню"**  
   - **Описание**: Открывает главное меню с доступными действиями.  
   - **Как использовать**: Нажмите кнопку "Меню" в ответном сообщении.  
   - **Результат**: Появится клавиатура с кнопками:  
     - **-** (добавить расход)  
     - **+** (добавить доход)  
     - **Статистика** (просмотр статистики)  
     - **Категории** (список категорий)  
     - **Экспорт** (экспорт данных в CSV)  
     - **Удалить** (удаление записей)  
     - **Часовой пояс** (установка часового пояса)  
     - **Инструкция** (показать эту инструкцию)

3. **Кнопка "Назад"**  
   - **Описание**: Возвращает в главное меню или отменяет текущее действие.  
   - **Как использовать**: Нажмите кнопку "Назад" в любой момент, когда она доступна.  
   - **Результат**: Вы вернетесь в главное меню, а текущее действие будет сброшено.

---

### Функции бота

#### 1. Добавление расхода (-)
   - **Описание**: Позволяет записать новый расход.  
   - **Как использовать**:  
     1. Нажмите кнопку **-** в меню.  
     2. Выберите категорию расхода (например, "Еда", "Транспорт", "Прочее") из предложенных кнопок.  
     3. Введите сумму и описание в формате: `<сумма> <описание>` (например, `500 Кофе`).  
   - **Результат**: Расход сохраняется в базе данных, и вы получите подтверждение с деталями.  
   - **Примечание**: Сумма должна быть положительным числом.

#### 2. Добавление дохода (+)
   - **Описание**: Позволяет записать новый доход.  
   - **Как использовать**:  
     1. Нажмите кнопку **+** в меню.  
     2. Выберите категорию дохода (например, "Зарплата", "Инвестиции", "Подарки") из предложенных кнопок.  
     3. Введите сумму и описание в формате: `<сумма> <описание>` (например, `5000 апрель`).  
   - **Результат**: Доход сохраняется в базе данных, и вы получите подтверждение с деталями.  
   - **Примечание**: Сумма должна быть положительным числом.

#### 3. Просмотр статистики (Статистика или /stats)
   - **Описание**: Отображает статистику расходов и доходов за день, неделю, месяц и год.  
   - **Как использовать**:  
     - Нажмите кнопку **Статистика** в меню или введите команду `/stats`.  
     - Альтернативно, введите букву `s` для краткой статистики.  
   - **Результат**: Бот покажет:  
     - Суммы расходов и доходов по категориям за каждый период.  
     - Итоговые суммы расходов и доходов.  
     - Баланс (доходы минус расходы).  
   - **Примечание**: Если данных за период нет, бот сообщит об этом.

#### 4. Просмотр категорий (Категории или /categories)
   - **Описание**: Показывает список доступных категорий расходов и доходов.  
   - **Как использовать**: Нажмите кнопку **Категории** в меню или введите `/categories`.  
   - **Результат**: Бот выведет список категорий, разделенных на расходы (например, "Еда", "Транспорт") и доходы (например, "Зарплата", "Инвестиции").  
   - **Примечание**: Категории предустановлены, но их можно расширить через базу данных.

#### 5. Экспорт данных (Экспорт или /export)
   - **Описание**: Экспортирует все ваши записи о расходах и доходах в CSV-файл.  
   - **Как использовать**: Нажмите кнопку **Экспорт** в меню или введите `/export`.  
   - **Результат**: Бот отправит CSV-файл с данными, содержащими все ваши транзакции (расходы и доходы).  
   - **Примечание**: Если данных нет, бот сообщит об этом.

#### 6. Удаление записей (Удалить или /delete)
   - **Описание**: Позволяет удалить конкретную запись по ID или полностью обнулить статистику.  
   - **Как использовать**:  
     1. Нажмите кнопку **Удалить** в меню или введите `/delete`.  
     2. Выберите одно из действий:  
        - **Удалить по ID**: Введите `/delete <id>` (например, `/delete 5`).  
        - **Обнулить статистику**: Подтвердите действие, чтобы удалить все ваши записи.  
     3. Если выбрано "Удалить по ID", укажите ID записи (его можно узнать из экспортированного CSV-файла).  
   - **Результат**:  
     - Для удаления по ID: указанная запись удаляется, если она принадлежит вам.  
     - Для обнуления: все ваши расходы и доходы удаляются.  
   - **Примечание**: ID должен быть числом, и запись должна существовать.

#### 7. Установка часового пояса (Часовой пояс или /settimezone)
   - **Описание**: Позволяет настроить часовой пояс для корректного учета времени транзакций.  
   - **Как использовать**:  
     1. Нажмите кнопку **Часовой пояс** в меню или введите `/settimezone`.  
     2. Введите название часового пояса в формате `Region/City` (например, `Europe/Moscow`).  
   - **Результат**: Часовой пояс сохраняется, и все новые транзакции будут записаны с учетом этого времени.  
   - **Примечание**: Если часовой пояс введен неверно, бот предложит повторить ввод. По умолчанию используется UTC.

#### 8. Инструкция (Инструкция или /instruction)
   - **Описание**: Показывает эту инструкцию с описанием всех функций бота.  
   - **Как использовать**: Нажмите кнопку **Инструкция** в меню или введите `/instruction`.  
   - **Результат**: Бот отправит полный текст инструкции.

---

### Полезные советы
- **Формат ввода транзакций**: Всегда указывайте сумму и описание через пробел (например, `1000 Подарок`).  
- **Проверка ID для удаления**: Чтобы узнать ID транзакций, экспортируйте данные через `/export`.  
- **Часовой пояс**: Убедитесь, что вы указали правильный часовой пояс, чтобы статистика отображалась корректно.  
- **Кнопка "Назад"**: Используйте её, чтобы отменить текущее действие и вернуться в меню.  
- **Ошибки**: Если бот сообщает об ошибке (например, неверный формат суммы), следуйте подсказкам в ответном сообщении.

---

### Пример взаимодействия
1. Вы вводите `/start` → появляется кнопка "Меню".  
2. Нажимаете "Меню" → выбираете **-** (расход).  
3. Выбираете категорию "Еда" → вводите `200 Обед`.  
4. Бот подтверждает: "Расход добавлен: Сумма: 200, Категория: Еда, Описание: Обед".  
5. Нажимаете **Статистика** → бот показывает статистику за день, неделю, месяц и год.  
6. Нажимаете **Экспорт** → получаете CSV-файл с данными.  
7. Нажимаете **Часовой пояс** → вводите `Europe/Moscow` → часовой пояс сохранен.  
8. Нажимаете **Инструкция** → получаете этот текст.

---

Если у вас возникнут вопросы, просто напишите боту, и он поможет разобраться!"""

async def init_db():
    async with aiosqlite.connect('expenses.db') as conn:
        c = await conn.cursor()
        await c.execute('''CREATE TABLE IF NOT EXISTS expenses
                          (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL, category TEXT, description TEXT, date TEXT)''')
        await c.execute('''CREATE TABLE IF NOT EXISTS incomes
                          (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL, category TEXT, description TEXT, date TEXT)''')
        await c.execute('''CREATE TABLE IF NOT EXISTS categories
                          (category TEXT PRIMARY KEY, type TEXT)''')
        await c.execute('''CREATE TABLE IF NOT EXISTS user_settings
                          (user_id INTEGER PRIMARY KEY, timezone TEXT)''')
        await c.execute('''CREATE TABLE IF NOT EXISTS user_id_mapping
                          (telegram_id INTEGER PRIMARY KEY, simple_id INTEGER)''')
        
        # Добавление индексов для ускорения запросов
        await c.execute('CREATE INDEX IF NOT EXISTS idx_expenses_user_id ON expenses (user_id)')
        await c.execute('CREATE INDEX IF NOT EXISTS idx_incomes_user_id ON incomes (user_id)')
        await c.execute('CREATE INDEX IF NOT EXISTS idx_expenses_date ON expenses (date)')
        await c.execute('CREATE INDEX IF NOT EXISTS idx_incomes_date ON incomes (date)')
        
        await c.execute('PRAGMA table_info(categories)')
        columns = [info[1] for info in await c.fetchall()]
        if 'type' not in columns:
            await c.execute('ALTER TABLE categories ADD COLUMN type TEXT')
            logging.info("Добавлен столбец 'type' в таблицу categories")
        
        default_categories = [
            ('Еда', 'expense'), ('Транспорт', 'expense'), ('Развлечения', 'expense'),
            ('Коммуналка', 'expense'), ('Прочее', 'expense'),
            ('Зарплата', 'income'), ('Инвестиции', 'income'), ('Подарки', 'income')
        ]
        await c.executemany('INSERT OR IGNORE INTO categories (category, type) VALUES (?, ?)', default_categories)
        
        await c.execute('SELECT category, type FROM categories')
        for category, type_ in await c.fetchall():
            CATEGORIES[type_].append(category)
        
        await conn.commit()

async def get_user_timezone(user_id):
    async with aiosqlite.connect('expenses.db') as conn:
        c = await conn.cursor()
        await c.execute('SELECT timezone FROM user_settings WHERE user_id = ?', (user_id,))
        result = await c.fetchone()
        try:
            return ZoneInfo(result[0] if result else DEFAULT_TIMEZONE)
        except ZoneInfoNotFoundError:
            logging.error(f"Неверный часовой пояс для user_id {user_id}, используется UTC")
            return ZoneInfo('UTC')

async def get_or_create_simple_id(telegram_id):
    if telegram_id in ID_MAPPING_CACHE:
        return ID_MAPPING_CACHE[telegram_id]
    
    async with aiosqlite.connect('expenses.db') as conn:
        c = await conn.cursor()
        await c.execute('SELECT simple_id FROM user_id_mapping WHERE telegram_id = ?', (telegram_id,))
        result = await c.fetchone()
        if result:
            ID_MAPPING_CACHE[telegram_id] = result[0]
            return result[0]
        else:
            logging.debug(f"Creating new simple_id for telegram_id {telegram_id}")
            await c.execute('SELECT MAX(simple_id) FROM user_id_mapping')
            max_id = (await c.fetchone())[0]
            simple_id = (max_id + 1) if max_id is not None else 1
            await c.execute('INSERT INTO user_id_mapping (telegram_id, simple_id) VALUES (?, ?)', (telegram_id, simple_id))
            await conn.commit()
            ID_MAPPING_CACHE[telegram_id] = simple_id
            logging.debug(f"Inserted simple_id {simple_id} for telegram_id {telegram_id}")
            return simple_id

async def update_user_ids_in_tables():
    async with aiosqlite.connect('expenses.db') as conn:
        c = await conn.cursor()
        await c.execute('SELECT DISTINCT user_id FROM expenses')
        expense_ids = [row[0] for row in await c.fetchall()]
        await c.execute('SELECT DISTINCT user_id FROM incomes')
        income_ids = [row[0] for row in await c.fetchall()]
        all_telegram_ids = set(expense_ids + income_ids)
        
        await c.execute('SELECT telegram_id, simple_id FROM user_id_mapping WHERE telegram_id IN ({})'.format(','.join('?' * len(all_telegram_ids))), list(all_telegram_ids))
        id_mapping = {row[0]: row[1] for row in await c.fetchall()}
        
        await c.execute('SELECT MAX(simple_id) FROM user_id_mapping')
        max_id = (await c.fetchone())[0]
        next_simple_id = (max_id + 1) if max_id is not None else 1
        
        for telegram_id in all_telegram_ids:
            if telegram_id not in id_mapping:
                id_mapping[telegram_id] = next_simple_id
                await c.execute('INSERT INTO user_id_mapping (telegram_id, simple_id) VALUES (?, ?)', (telegram_id, next_simple_id))
                ID_MAPPING_CACHE[telegram_id] = next_simple_id
                next_simple_id += 1
        
        for telegram_id, simple_id in id_mapping.items():
            await c.execute('UPDATE expenses SET user_id = ? WHERE user_id = ?', (simple_id, telegram_id))
            await c.execute('UPDATE incomes SET user_id = ? WHERE user_id = ?', (simple_id, telegram_id))
        
        await conn.commit()

def get_back_keyboard():
    return types.ReplyKeyboardMarkup(
        keyboard=[[types.KeyboardButton(text="Назад")]],
        resize_keyboard=True
    )

class TransactionForm(StatesGroup):
    choosing_category = State()
    entering_amount = State()

class DeleteForm(StatesGroup):
    choosing_action = State()
    entering_id = State()

class TimezoneForm(StatesGroup):
    entering_timezone = State()

def is_expense_command(message: types.Message):
    return message.text and message.text.startswith('-')

def is_income_command(message: types.Message):
    return message.text and message.text.startswith('+')

def is_stats_command(message: types.Message):
    return message.text and message.text.startswith('s') and len(message.text) == 1

@router.message(Command(commands=['start']))
async def send_welcome(message: types.Message):
    logging.debug(f"Received /start from user {message.from_user.id}")
    await get_or_create_simple_id(message.from_user.id)
    keyboard = types.ReplyKeyboardMarkup(
        keyboard=[[types.KeyboardButton(text="Меню")]],
        resize_keyboard=True
    )
    await message.reply("Добро пожаловать! Нажмите 'Меню' для начала работы.", reply_markup=keyboard)

@router.message(lambda message: message.text == "Меню")
async def show_menu(message: types.Message):
    keyboard = types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="-"), types.KeyboardButton(text="+")],
            [types.KeyboardButton(text="Статистика"), types.KeyboardButton(text="Категории")],
            [types.KeyboardButton(text="Экспорт"), types.KeyboardButton(text="Удалить")],
            [types.KeyboardButton(text="Часовой пояс"), types.KeyboardButton(text="Инструкция")]
        ],
        resize_keyboard=True
    )
    await message.reply("Выберите действие:", reply_markup=keyboard)

@router.message(lambda message: message.text == "Назад")
async def go_back(message: types.Message, state: FSMContext):
    await state.clear()
    await show_menu(message)

@router.message(lambda message: message.text in ["Статистика", "Категории", "Экспорт", "Удалить", "Часовой пояс", "Инструкция"])
async def handle_menu_action(message: types.Message, state: FSMContext):
    text = message.text
    if text == "Статистика":
        await show_stats(message, detailed=False)
    elif text == "Категории":
        await list_categories(message)
    elif text == "Экспорт":
        await export_csv(message)
    elif text == "Удалить":
        await start_delete(message, state)
    elif text == "Часовой пояс":
        await start_set_timezone(message, state)
    elif text == "Инструкция":
        await show_instruction(message)

@router.message(Command(commands=['instruction']))
async def show_instruction(message: types.Message):
    MAX_MESSAGE_LENGTH = 4000
    parts = []
    current_part = ""
    for line in INSTRUCTION_TEXT.split("\n"):
        if len(current_part) + len(line) + 1 > MAX_MESSAGE_LENGTH:
            parts.append(current_part)
            current_part = line + "\n"
        else:
            current_part += line + "\n"
    if current_part:
        parts.append(current_part)
    
    for i, part in enumerate(parts):
        reply_markup = get_back_keyboard() if i == len(parts) - 1 else None
        await message.reply(part, parse_mode="Markdown", reply_markup=reply_markup)

@router.message(Command(commands=['settimezone']))
async def start_set_timezone(message: types.Message, state: FSMContext):
    await state.set_state(TimezoneForm.entering_timezone)
    await message.reply("Введите часовой пояс (например, Europe/Moscow):", reply_markup=get_back_keyboard())

@router.message(TimezoneForm.entering_timezone)
async def process_timezone(message: types.Message, state: FSMContext):
    timezone = message.text
    if timezone == "Назад":
        await go_back(message, state)
        return
    try:
        ZoneInfo(timezone)
        simple_id = await get_or_create_simple_id(message.from_user.id)
        async with aiosqlite.connect('expenses.db') as conn:
            c = await conn.cursor()
            await c.execute('INSERT OR REPLACE INTO user_settings (user_id, timezone) VALUES (?, ?)',
                            (simple_id, timezone))
            await conn.commit()
        await message.reply(f"Часовой пояс установлен: {timezone}", reply_markup=get_back_keyboard())
        await state.clear()
    except ZoneInfoNotFoundError:
        await message.reply("Неверный часовой пояс. Попробуйте снова (например, Europe/Moscow).", reply_markup=get_back_keyboard())
    except Exception as e:
        await message.reply(f"Произошла ошибка: {str(e)}", reply_markup=get_back_keyboard())
        await state.clear()

@router.message(Command(commands=['categories']))
async def list_categories(message: types.Message):
    if CATEGORIES['expense'] or CATEGORIES['income']:
        response = "Доступные категории:\nРасходы:\n" + "\n".join(f"{cat} (расход)" for cat in CATEGORIES['expense']) + \
                   "\nДоходы:\n" + "\n".join(f"{cat} (доход)" for cat in CATEGORIES['income'])
        await message.reply(response, reply_markup=get_back_keyboard())
    else:
        await message.reply("Категории не найдены.", reply_markup=get_back_keyboard())

@router.message(is_expense_command)
async def start_expense(message: types.Message, state: FSMContext):
    await state.update_data(action='expense')
    categories = CATEGORIES['expense']
    keyboard = types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text=cat) for cat in categories[i:i + 2]] for i in range(0, len(categories), 2)
        ] + [[types.KeyboardButton(text="Назад")]],
        resize_keyboard=True
    )
    await state.set_state(TransactionForm.choosing_category)
    await message.reply("Выберите категорию для расхода:", reply_markup=keyboard)

@router.message(is_income_command)
async def start_income(message: types.Message, state: FSMContext):
    await state.update_data(action='income')
    categories = CATEGORIES['income']
    keyboard = types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text=cat) for cat in categories[i:i + 2]] for i in range(0, len(categories), 2)
        ] + [[types.KeyboardButton(text="Назад")]],
        resize_keyboard=True
    )
    await state.set_state(TransactionForm.choosing_category)
    await message.reply("Выберите категорию для дохода:", reply_markup=keyboard)

@router.message(TransactionForm.choosing_category)
async def enter_amount(message: types.Message, state: FSMContext):
    if message.text == "Назад":
        await go_back(message, state)
        return
    category = message.text
    if category not in CATEGORIES['expense'] and category not in CATEGORIES['income']:
        await message.reply("Пожалуйста, выберите категорию из предложенных.", reply_markup=get_back_keyboard())
        return
    await state.update_data(category=category)
    await state.set_state(TransactionForm.entering_amount)
    example = "500 Кофе"
    if category == "Зарплата":
        example = "5000 апрель"
    elif category == "Инвестиции":
        example = "500 акция"
    await message.reply(f"Введите сумму и описание (например, {example}):", reply_markup=types.ReplyKeyboardRemove())

@router.message(TransactionForm.entering_amount)
async def save_transaction(message: types.Message, state: FSMContext):
    try:
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            await message.reply("Неверный формат. Используйте: <сумма> <описание>", reply_markup=get_back_keyboard())
            return
        amount = float(parts[0])
        if amount <= 0:
            raise ValueError("Сумма должна быть положительной!")
        description = parts[1]
        
        data = await state.get_data()
        action = data['action']
        category = data['category']
        telegram_id = message.from_user.id
        simple_id = await get_or_create_simple_id(telegram_id)
        tz = await get_user_timezone(simple_id)
        date = datetime.now(tz=tz).strftime('%Y-%m-%d %H:%M:%S')
        
        async with aiosqlite.connect('expenses.db') as conn:
            c = await conn.cursor()
            table = 'expenses' if action == 'expense' else 'incomes'
            await c.execute(f'INSERT INTO {table} (user_id, amount, category, description, date) VALUES (?, ?, ?, ?, ?)',
                            (simple_id, amount, category, description, date))
            await conn.commit()
        action_text = "Расход" if action == 'expense' else "Доход"
        await message.reply(
            f"{action_text} добавлен:\nСумма: {amount}\nКатегория: {category}\nОписание: {description}",
            reply_markup=get_back_keyboard()
        )
        await state.clear()
    except ValueError as e:
        await message.reply(f"Ошибка: {str(e)}. Убедитесь, что сумма — число.", reply_markup=get_back_keyboard())
    except Exception as e:
        await message.reply(f"Произошла ошибка: {str(e)}", reply_markup=get_back_keyboard())

@router.message(Command(commands=['stats']))
async def show_stats(message: types.Message, detailed: bool = False):
    telegram_id = message.from_user.id
    simple_id = await get_or_create_simple_id(telegram_id)
    tz = await get_user_timezone(simple_id)
    now = datetime.now(tz)
    
    day_start = now.replace(hour=DAY_START_HOUR, minute=0, second=0, microsecond=0)
    if now.hour < DAY_START_HOUR:
        day_start -= timedelta(days=1)
    periods = {
        'день': day_start,
        'неделю': now - timedelta(days=now.weekday()),
        'месяц': now.replace(day=1, hour=DAY_START_HOUR, minute=0, second=0, microsecond=0),
        'год': now.replace(month=1, day=1, hour=DAY_START_HOUR, minute=0, second=0, microsecond=0)
    }
    
    response = "Статистика расходов и доходов:\n"
    async with aiosqlite.connect('expenses.db') as conn:
        c = await conn.cursor()
        for period_name, start_date in periods.items():
            start_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
            response += f"\nЗа последний {period_name}:\n"
            
            await c.execute('SELECT category, SUM(amount) FROM expenses WHERE user_id = ? AND date >= ? GROUP BY category',
                            (simple_id, start_date_str))
            expenses = await c.fetchall()
            total_expenses = sum(row[1] for row in expenses) if expenses else 0
            
            await c.execute('SELECT category, SUM(amount) FROM incomes WHERE user_id = ? AND date >= ? GROUP BY category',
                            (simple_id, start_date_str))
            incomes = await c.fetchall()
            total_incomes = sum(row[1] for row in incomes) if incomes else 0
            
            if expenses:
                response += "Расходы:\n"
                for category, amount in expenses:
                    response += f"{category}: {amount:.2f}\n"
            if incomes:
                response += "Доходы:\n"
                for category, amount in incomes:
                    response += f"{category}: {amount:.2f}\n"
            response += f"Итого расходы: {total_expenses:.2f}\n"
            response += f"Итого доходы: {total_incomes:.2f}\n"
            response += f"Баланс: {total_incomes - total_expenses:.2f}\n"
            
            if detailed and period_name == 'неделю':
                await c.execute('SELECT strftime("%Y-%m-%d", date) as day, SUM(amount) FROM expenses WHERE user_id = ? AND date >= ? GROUP BY day',
                                (simple_id, start_date_str))
                daily_expenses = await c.fetchall()
                await c.execute('SELECT strftime("%Y-%m-%d", date) as day, SUM(amount) FROM incomes WHERE user_id = ? AND date >= ? GROUP BY day',
                                (simple_id, start_date_str))
                daily_incomes = await c.fetchall()
                if daily_expenses or daily_incomes:
                    response += "\nПодробно по дням:\n"
                    days = set([row[0] for row in daily_expenses] + [row[0] for row in daily_incomes])
                    for day in sorted(days):
                        day_exp = sum(row[1] for row in daily_expenses if row[0] == day) if any(row[0] == day for row in daily_expenses) else 0
                        day_inc = sum(row[1] for row in daily_incomes if row[0] == day) if any(row[0] == day for row in daily_incomes) else 0
                        response += f"{day}: Расходы {day_exp:.2f}, Доходы {day_inc:.2f}, Баланс {day_inc - day_exp:.2f}\n"
            elif detailed and period_name == 'год':
                await c.execute('SELECT strftime("%Y-%m", date) as month, SUM(amount) FROM expenses WHERE user_id = ? AND date >= ? GROUP BY month',
                                (simple_id, start_date_str))
                monthly_expenses = await c.fetchall()
                await c.execute('SELECT strftime("%Y-%m", date) as month, SUM(amount) FROM incomes WHERE user_id = ? AND date >= ? GROUP BY month',
                                (simple_id, start_date_str))
                monthly_incomes = await c.fetchall()
                if monthly_expenses or monthly_incomes:
                    response += "\nПодробно по месяцам:\n"
                    months = set([row[0] for row in monthly_expenses] + [row[0] for row in monthly_incomes])
                    for month in sorted(months):
                        month_exp = sum(row[1] for row in monthly_expenses if row[0] == month) if any(row[0] == month for row in monthly_expenses) else 0
                        month_inc = sum(row[1] for row in monthly_incomes if row[0] == month) if any(row[0] == month for row in monthly_incomes) else 0
                        response += f"{month}: Расходы {month_exp:.2f}, Доходы {month_inc:.2f}, Баланс {month_inc - month_exp:.2f}\n"
    
    if "За последний день:" in response and total_expenses == 0 and total_incomes == 0:
        await message.reply("Нет данных для отображения статистики.", reply_markup=get_back_keyboard())
    else:
        await message.reply(response, reply_markup=get_back_keyboard())

@router.message(is_stats_command)
async def show_stats_short(message: types.Message):
    await show_stats(message, detailed=False)

@router.message(Command(commands=['export']))
async def export_csv(message: types.Message):
    telegram_id = message.from_user.id
    simple_id = await get_or_create_simple_id(telegram_id)
    with sqlite3.connect('expenses.db') as conn:
        df_expenses = pd.read_sql_query('SELECT * FROM expenses WHERE user_id = ?', conn, params=(simple_id,))
        df_incomes = pd.read_sql_query('SELECT * FROM incomes WHERE user_id = ?', conn, params=(simple_id,))
        
        if df_expenses.empty and df_incomes.empty:
            await message.reply("Нет данных для экспорта.", reply_markup=get_back_keyboard())
            return
        
        dfs = []
        if not df_expenses.empty:
            dfs.append(df_expenses.assign(type='расход'))
        if not df_incomes.empty:
            dfs.append(df_incomes.assign(type='доход'))
        
        if dfs:
            df_all = pd.concat(dfs, ignore_index=True)
            df_all = df_all.rename(columns={
                'user_id': 'ИД_пользователя',
                'amount': 'Сумма',
                'category': 'Категория',
                'description': 'Описание',
                'date': 'Дата',
                'type': 'Тип'
            })
            csv_file = f'транзакции_{simple_id}.csv'
            async with aiofiles.open(csv_file, 'w', encoding='utf-8-sig') as f:
                await f.write('\ufeff')
                await f.write(df_all.to_csv(index=False, sep=';'))
            async with aiofiles.open(csv_file, 'rb') as file:
                content = await file.read()
                input_file = types.BufferedInputFile(content, filename=csv_file)
                await message.reply_document(document=input_file, caption="Ваши расходы и доходы в CSV", reply_markup=get_back_keyboard())
            os.remove(csv_file)
        else:
            await message.reply("Нет данных для экспорта.", reply_markup=get_back_keyboard())

@router.message(Command(commands=['delete']))
async def start_delete(message: types.Message, state: FSMContext):
    keyboard = types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="Удалить по ID"), types.KeyboardButton(text="Обнулить статистику")],
            [types.KeyboardButton(text="Назад")]
        ],
        resize_keyboard=True
    )
    await state.set_state(DeleteForm.choosing_action)
    await message.reply("Выберите действие:", reply_markup=keyboard)

@router.message(DeleteForm.choosing_action)
async def process_delete_action(message: types.Message, state: FSMContext):
    action = message.text
    telegram_id = message.from_user.id
    simple_id = await get_or_create_simple_id(telegram_id)
    if action == "Удалить по ID":
        await state.set_state(DeleteForm.entering_id)
        await message.reply("Введите /delete <id> для удаления записи.", reply_markup=get_back_keyboard())
    elif action == "Обнулить статистику":
        async with aiosqlite.connect('expenses.db') as conn:
            c = await conn.cursor()
            await c.execute('DELETE FROM expenses WHERE user_id = ?', (simple_id,))
            await c.execute('DELETE FROM incomes WHERE user_id = ?', (simple_id,))
            await conn.commit()
        await message.reply("Вся ваша статистика обнулена.", reply_markup=get_back_keyboard())
        await state.clear()
    elif action == "Назад":
        await go_back(message, state)
    else:
        await message.reply("Пожалуйста, выберите действие из предложенных.", reply_markup=get_back_keyboard())

@router.message(DeleteForm.entering_id)
async def delete_transaction(message: types.Message, state: FSMContext):
    try:
        parts = message.text.split()
        if len(parts) != 2 or parts[0] != "/delete":
            await message.reply("Неверный формат. Используйте: /delete <id>", reply_markup=get_back_keyboard())
            return
        transaction_id = int(parts[1])
        
        telegram_id = message.from_user.id
        simple_id = await get_or_create_simple_id(telegram_id)
        
        async with aiosqlite.connect('expenses.db') as conn:
            c = await conn.cursor()
            await c.execute('DELETE FROM expenses WHERE id = ? AND user_id = ?', (transaction_id, simple_id))
            if c.rowcount > 0:
                await conn.commit()
                await message.reply(f"Запись с ID {transaction_id} удалена из расходов.", reply_markup=get_back_keyboard())
                await state.clear()
                return
            
            await c.execute('DELETE FROM incomes WHERE id = ? AND user_id = ?', (transaction_id, simple_id))
            if c.rowcount > 0:
                await conn.commit()
                await message.reply(f"Запись с ID {transaction_id} удалена из доходов.", reply_markup=get_back_keyboard())
                await state.clear()
                return
            
            await message.reply(f"Запись с ID {transaction_id} не найдена.", reply_markup=get_back_keyboard())
            await state.clear()
    except ValueError:
        await message.reply("ID должен быть числом.", reply_markup=get_back_keyboard())
    except Exception as e:
        await message.reply(f"Произошла ошибка: {str(e)}", reply_markup=get_back_keyboard())
    finally:
        await state.clear()

dp.include_router(router)

async def main():
    await init_db()
    await update_user_ids_in_tables()
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())