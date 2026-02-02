import logging
from collections import defaultdict
from datetime import datetime, timedelta

import aiomysql
from aiogram import Bot, Dispatcher, types
from aiogram.exceptions import TelegramForbiddenError
from aiogram.filters import Filter, Command
from aiogram.types import Message, KeyboardButton, ReplyKeyboardMarkup, BotCommand, MenuButtonCommands
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import (
    Command,
    CommandObject,
    ChatMemberUpdatedFilter,
    IS_NOT_MEMBER,
    MEMBER,
    IS_MEMBER,
    LEFT,
    KICKED,
)
from aiogram.types import (
    Message,
    ChatPermissions,
    ChatMemberUpdated,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.filters import BaseFilter
from aiogram.enums import ParseMode  # Прямой импорт ParseMode
from aiogram.client.default import DefaultBotProperties
import asyncio
from fastapi import FastAPI, Request
import uvicorn
from vk_api import VkApi
from vk_api.upload import VkUpload
from vk_api.exceptions import ApiError

import sys
import io

# Устанавливаем кодировку консоли на utf-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Настройки MySQL
MYSQL_HOST = 'fvh2.spaceweb.ru'
MYSQL_PORT = 3306
MYSQL_USER = 'hlebopash2'
MYSQL_PASSWORD = 'Hlebopashev2025'
MYSQL_DB = 'hlebopash2'

# Токен вашего бота
API_TOKEN = '7657074513:AAHtBlV7DScui4RnPI2VfV7Zl1O0D0JL2rU'

# Настройки ВКонтакте
VK_ACCESS_TOKEN = 'df9915eedf9915eedf9915ee33dcb3b6d5ddf99df9915eeb821e30ecc1e3e872bdaf59b'
VK_GROUP_ID = '229287670'

# Инициализация бота с DefaultBotProperties
bot = Bot(token=API_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN_V2))
dp = Dispatcher()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding='utf-8'),  # Логи в файл с кодировкой utf-8
        logging.StreamHandler()                           # Логи в консоль
    ]
)
logger = logging.getLogger(__name__)

# Инициализация VK API
vk_session = VkApi(token=VK_ACCESS_TOKEN)
vk = vk_session.get_api()
vk_upload = VkUpload(vk_session)

# Ключевые слова для игнорирования (можно добавить больше)
IGNORE_KEYWORDS = ["Чатик", "чатик", "Chatik", "chatik"]

# Хэштеги для пересылки
HASHTAGS = ["#нейросети", "#программирование", "#разработка_игр"]

# # Обработчик ключевого слова "Техноголик"
# @dp.message(TextFilter("Техноголик"))
# async def technogolik_response(message: Message):
#     await message.reply(
#         "Привет. Я - Техноголик, живу здесь для модерации и развлечений.",
#         parse_mode=ParseMode.MARKDOWN_V2
#     )

# Функция для подключения к MySQL
async def get_mysql_connection():
    return await aiomysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        db=MYSQL_DB,
        cursorclass=aiomysql.DictCursor
    )

# Функция для создания таблицы (если её нет)
async def create_tables():
    async with await get_mysql_connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT NOT NULL UNIQUE,
                    username VARCHAR(255),
                    referral_code VARCHAR(50) UNIQUE,
                    invited_by BIGINT,
                    invite_count INT DEFAULT 0
                )
            """)
            await conn.commit()

# Функция для создания таблицы users_list, если её нет
async def create_users_table():
    async with await get_mysql_connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS users_list (
                    user_id BIGINT PRIMARY KEY,
                    username VARCHAR(255)
                )
            """)
            await conn.commit()

# Асинхронная функция для регистрации пользователя
async def register_user(user_id: int, username: str, referral_code: str, invited_by: int = None):
    try:
        async with await get_mysql_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "INSERT INTO users (user_id, username, referral_code, invited_by) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE username = %s",
                    (user_id, username, referral_code, invited_by, username)
                )
                await conn.commit()
                logger.info(f"Пользователь {user_id} зарегистрирован")
    except Exception as e:
        logger.error(f"Ошибка при регистрации пользователя: {e}")

# Функция для получения данных пользователя
async def get_user(user_id: int):
    async with await get_mysql_connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
            return await cursor.fetchone()

# Функция для обновления счетчика приглашений
async def update_invite_count(user_id: int):
    try:
        async with await get_mysql_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("UPDATE users SET invite_count = invite_count + 1 WHERE user_id = %s", (user_id,))
                await conn.commit()
    except Exception as e:
        logger.error(f"Ошибка при обновлении счетчика приглашений для пользователя {user_id}: {e}")

# ID канала, на который нужно подписаться
CHANNEL_ID = "@technogoliki"  # Убедитесь, что это правильный username или ID канала

# ID канала "Чатик" (замените на актуальный ID)
CHATIK_CHANNEL_ID = "@technogoliki/121"  # Пример ID канала

CHANNEL_USERNAME = "technogoliki"

# Права для мута (запрет на отправку сообщений)
MUTE_PERMISSIONS = ChatPermissions(
    can_send_messages=False,  # Запрет на отправку сообщений
    can_send_media_messages=False,  # Запрет на отправку медиа
    can_send_polls=False,  # Запрет на создание опросов
    can_send_other_messages=False,  # Запрет на другие типы сообщений
    can_add_web_page_previews=False,  # Запрет на превью ссылок
    can_change_info=False,  # Запрет на изменение информации чата
    can_invite_users=False,  # Запрет на приглашение пользователей
    can_pin_messages=False,  # Запрет на закрепление сообщений
)

# Права для размута (возврат стандартных прав)
UNMUTE_PERMISSIONS = ChatPermissions(
    can_send_messages=True,
    can_send_media_messages=True,
    can_send_polls=True,
    can_send_other_messages=True,
    can_add_web_page_previews=True,
    can_change_info=False,
    can_invite_users=True,
    can_pin_messages=False,
)

# Словарь для хранения количества предупреждений (warns) пользователей
user_warns = defaultdict(int)

# Словарь для отслеживания активности пользователей (антиспам)
user_message_count = defaultdict(int)
user_last_message_time = defaultdict(lambda: datetime.now())

# Функция для экранирования зарезервированных символов в MarkdownV2
def escape_markdown(text: str) -> str:
    reserved_chars = r"\_*[]()~`>#+-=|{}.!"
    for char in reserved_chars:
        text = text.replace(char, f"\\{char}")
    return text

# Функция для оформления текста как цитаты
def quote_text(text: str) -> str:
    return f"> {escape_markdown(text)}"

# Функция для создания реферальной ссылки
async def create_referral_link(user_id: int):
    referral_code = f"REF{user_id}"
    return f"https://t\\.me/technogolik_IT_bot?start={referral_code}"

# Функция для проверки подписки на канал
async def check_subscription(user_id: int) -> bool:
    try:
        chat_member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        return chat_member.status in ["member", "administrator", "creator"]
    except Exception as e:
        logger.error(f"Ошибка при проверке подписки: {e}")
        return False


# Функция для поиска пользователя по реферальному коду
async def get_user_by_referral_code(referral_code: str):
    try:
        async with await get_mysql_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT * FROM users WHERE referral_code = %s", (referral_code,))
                result = await cursor.fetchone()
                return result
    except Exception as e:
        logger.error(f"Ошибка при поиске пользователя по реферальному коду {referral_code}: {e}")
        return None

# Создаем клавиатуру с кнопкой "Предложка"
suggest_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Предложка")]
    ],
    resize_keyboard=True  # Автоматически подгоняет размер кнопок
)

# Настройка клавиатуры по умолчанию при запуске бота
async def on_startup(dispatcher):

    if await check_vk_connection():
        logger.info("Бот успешно запущен и подключен к группе ВК")
    else:
        logger.error("Бот запущен, но не подключен к группе ВК")

    # Устанавливаем команды для меню бота
    await bot.set_my_commands([
        BotCommand(command="predlozhka", description="Открыть предложку")
    ])
    # Устанавливаем кнопку меню
    await bot.set_chat_menu_button(menu_button=MenuButtonCommands(type="commands"))



# Функция для получения реферальной информации
async def my_ref(user_id: int):
    try:
        async with await get_mysql_connection() as conn:
            async with conn.cursor() as cursor:
                # Получаем данные пользователя
                await cursor.execute("SELECT referral_code, invite_count FROM users WHERE user_id = %s", (user_id,))
                user_data = await cursor.fetchone()

                if not user_data:
                    return {"error": "Пользователь не найден. Пожалуйста, зарегистрируйтесь с помощью команды /start."}

                # Получаем список приглашённых пользователей
                await cursor.execute("SELECT username FROM users WHERE invited_by = %s", (user_id,))
                invited_users = await cursor.fetchall()

                # Формируем список приглашённых
                invited_list = [user['username'] for user in invited_users]

                # Возвращаем данные в правильном формате
                return {
                    "referral_code": user_data['referral_code'],
                    "invited_users": invited_list,
                    "invite_count": user_data['invite_count']
                }
    except Exception as e:
        logger.error(f"Ошибка при получении реферальной информации для пользователя {user_id}: {e}")
        return {"error": "Ошибка при обработке запроса. Пожалуйста, попробуйте позже."}

# Обработчик команды /predlozhka
@dp.message(Command("predlozhka"))
async def cmd_predlozhka(message: Message):
    # Отправляем сообщение с нумерованным списком тем
    await message.answer(
        "Введите номер пункта, который соответствует той теме, в которую вы хотите предложить пост:\n"
        "1\\. Нейросети\n"
        "2\\. Программирование\n"
        "3\\. Технологии\n"
        "4\\. Разработка игр"
    )

# Обработчик ввода номера темы
@dp.message(F.text.in_({"1", "2", "3", "4"}))
async def process_topic_choice(message: Message):
    # Определяем тему по введённому номеру
    topic_map = {
        "1": "Нейросети",
        "2": "Программирование",
        "3": "Технологии",
        "4": "Разработка игр"
    }
    topic = topic_map[message.text]

    # Отправляем сообщение с выбранной темой
    await message.answer(f"Вы выбрали тему: {topic}\\. Теперь отправьте ваш пост\\.")

# Обработчик команды /start
@dp.message(Command("start"))
async def cmd_start(message: Message, command: CommandObject):

    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    btn1 = types.KeyboardButton("? Поздороваться")
    btn2 = types.KeyboardButton("❓ Задать вопрос")
    markup.add(btn1, btn2)

    user_id = message.from_user.id
    username = message.from_user.full_name

    # Извлекаем реферальный код из команды /start
    referral_code = command.args  # Аргумент после /start

    # Если есть реферальный код, сохраняем его в базе данных
    if referral_code and referral_code.startswith("REF"):
        invited_by = int(referral_code.replace("REF", ""))  # Извлекаем ID пригласившего
    else:
        # Если реферального кода нет, пользователь сам становится рефералом
        invited_by = None

    # Создаем реферальный код и ссылку для текущего пользователя
    current_referral_code = f"REF{user_id}"  # Реферальный код = REF + user_id
    current_referral_link = f"https://t.me/technogoliki_IT_bot?start={current_referral_code}"  # Реферальная ссылка

    try:
        async with await get_mysql_connection() as conn:
            async with conn.cursor() as cursor:
                # Вставляем новую запись в таблицу users
                await cursor.execute(
                    """
                    INSERT INTO users (user_id, username, referral_code, invited_by, invite_count, referral_link)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (user_id, username, current_referral_code, invited_by, 0, current_referral_link)
                )
                await conn.commit()
                logger.info(f"Новая запись добавлена: user_id={user_id}, username={username}, referral_code={current_referral_code}, invited_by={invited_by}, referral_link={current_referral_link}")
    except Exception as e:
        logger.error(f"Ошибка при добавлении новой записи: {e}")
        logger.error(f"Тип ошибки: {type(e).__name__}, Сообщение: {str(e)}")

    # Создаем кнопку "Подписаться" с переходом в канал (в раздел с темами)
    subscribe_button = InlineKeyboardButton(
        text="Подписаться", 
        url=f"https://t.me/{CHANNEL_USERNAME}"  # Переход в канал (раздел с темами)
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[subscribe_button]])

    # Отправляем сообщение с кнопкой
    await message.answer(
        "Добро пожаловать\\! Чтобы продолжить, подпишитесь на наш канал:",
        reply_markup=keyboard
    )

# Обработчик события подписки на канал
@dp.chat_member(ChatMemberUpdatedFilter(IS_NOT_MEMBER >> MEMBER))
async def handle_chat_member_update(event: ChatMemberUpdated):
    user_id = event.new_chat_member.user.id
    username = event.new_chat_member.user.username or event.new_chat_member.user.full_name

    # Логируем событие подписки
    logger.info(f"Пользователь {user_id} ({username}) подписался на канал {event.chat.id}")

    # Отправляем приветственное сообщение
    await event.answer(
        f"> [{escape_markdown(username)}](tg://user?id={user_id}) присоединился к нам\\. 🎉",
        parse_mode=ParseMode.MARKDOWN_V2
    )

    # Проверяем, был ли пользователь приглашен через реферальную ссылку
    try:
        async with await get_mysql_connection() as conn:
            async with conn.cursor() as cursor:
                # Получаем invited_by из таблицы users
                await cursor.execute("SELECT invited_by FROM users WHERE user_id = %s", (user_id,))
                user_data = await cursor.fetchone()
                logger.info(f"Результат запроса invited_by: {user_data}")

                # Если пользователь был приглашен (invited_by не NULL), обрабатываем событие
                if user_data and user_data['invited_by']:
                    invited_by_user_id = user_data['invited_by']
                    logger.info(f"Пользователь {user_id} был приглашен пользователем {invited_by_user_id}")

                    # Добавляем пользователя в users_list
                    await cursor.execute(
                        "INSERT INTO users_list (user_id, username) VALUES (%s, %s) ON DUPLICATE KEY UPDATE username = %s",
                        (user_id, username, username)
                    )
                    await conn.commit()
                    logger.info(f"Пользователь {user_id} ({username}) добавлен в users_list")

                    # Считаем количество приглашенных пользователей для invited_by_user_id
                    await cursor.execute("SELECT COUNT(*) as invite_count FROM users WHERE invited_by = %s", (invited_by_user_id,))
                    invite_count_result = await cursor.fetchone()
                    new_invite_count = invite_count_result['invite_count']
                    logger.info(f"Новое значение invite_count для пользователя {invited_by_user_id}: {new_invite_count}")

                    # Обновляем invite_count у пригласившего
                    await cursor.execute(
                        "UPDATE users SET invite_count = %s WHERE user_id = %s",
                        (new_invite_count, invited_by_user_id)
                    )
                    await conn.commit()
                    logger.info(f"Счетчик приглашений обновлен для пользователя {invited_by_user_id} (новое значение: {new_invite_count})")
                else:
                    logger.info(f"Пользователь {user_id} не был приглашен через реферальную ссылку (invited_by отсутствует)")
    except Exception as e:
        logger.error(f"Ошибка при обработке события подписки: {e}")

# Обработчик команды /rassilka
@dp.message(Command("rassilka"))
async def cmd_rassilka(message: Message):
    # Проверяем, что команду вызвал администратор
    if message.from_user.id != 1981956063:  # Замените ADMIN_ID на ID администратора
        await message.answer("Эта команда доступна только администратору\\.")
        return

    # Получаем текст рассылки
    text = message.text.replace("/rassilka", "").strip()
    if not text:
        await message.answer("Пожалуйста, укажите текст для рассылки в формате: /rassilka <текст>")
        return

    # Логируем начало рассылки
    print("Начало рассылки...")
    logger.info("Начало рассылки...")

    # Получаем список всех пользователей из таблицы users_list
    try:
        async with await get_mysql_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT user_id, username FROM users_list")
                users = await cursor.fetchall()
    except Exception as e:
        logger.error(f"Ошибка при получении пользователей из базы данных: {e}")
        await message.answer("Не удалось получить список пользователей\\.")
        return

    # Выводим список всех участников
    print("Список участников:")
    for user in users:
        print(f"User ID: {user['user_id']}, Username: {user['username']}")

    # Отправляем сообщение каждому пользователю
    success_count = 0
    fail_count = 0
    for user in users:
        try:
            await bot.send_message(chat_id=user['user_id'], text=text)
            success_count += 1
            print(f"Сообщение успешно отправлено пользователю {user['user_id']} ({user['username']})")
            logger.info(f"Сообщение успешно отправлено пользователю {user['user_id']} ({user['username']})")
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение пользователю {user['user_id']} ({user['username']}): {e}")
            fail_count += 1
            print(f"Ошибка при отправке сообщения пользователю {user['user_id']} ({user['username']}): {e}")

    # Логируем завершение рассылки
    print(f"Рассылка завершена\\. Успешно отправлено: {success_count}, Не удалось отправить: {fail_count}")
    logger.info(f"Рассылка завершена\\. Успешно отправлено: {success_count}, Не удалось отправить: {fail_count}")

    # Отправляем отчет администратору
    await message.answer(f"Рассылка завершена\\.\nУспешно отправлено: {success_count}\nНе удалось отправить: {fail_count}")

# Обработчик команды /my_ref
@dp.message(Command("my_ref"))
async def cmd_my_ref(message: Message):
    user_id = message.from_user.id

    # Получаем реферальную информацию
    async with await get_mysql_connection() as conn:
        async with conn.cursor() as cursor:
            # Получаем данные пользователя
            await cursor.execute("SELECT referral_code, invite_count FROM users WHERE user_id = %s", (user_id,))
            user_data = await cursor.fetchone()

            if not user_data:
                await message.answer("Пользователь не найден\\. Пожалуйста, зарегистрируйтесь с помощью команды /start\\.", parse_mode=ParseMode.HTML)
                return

            referral_code = user_data['referral_code']
            invite_count = user_data['invite_count']

            # Получаем список приглашённых пользователей
            await cursor.execute("SELECT username FROM users WHERE invited_by = %s", (user_id,))
            invited_users = await cursor.fetchall()

            # Формируем сообщение с реферальной информацией
            ref_message = (
                f"Ваш реферальный код: <code>{referral_code}</code>\n"
                f"Ваша реферальная ссылка: https://t.me/technogolik_IT_bot?start={referral_code}\n"
                f"Вы пригласили {invite_count} пользователей:\n"
            )
            if invited_users:
                ref_message += "\n".join(f"- {user['username']}" for user in invited_users)
            else:
                ref_message += "Пока никого не пригласили."

            # Отправляем сообщение в режиме HTML
            await message.answer(ref_message, parse_mode=ParseMode.HTML)

# Обработчик команды /ref_stats
@dp.message(Command("ref_stats"))
async def cmd_ref_stats(message: Message):
    user_id = message.from_user.id

    # Получаем статистику по приглашённым пользователям
    async with await get_mysql_connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT invite_count FROM users WHERE user_id = %s", (user_id,))
            result = await cursor.fetchone()

    if result:
        invite_count = result['invite_count']
        # Отправляем сообщение в режиме HTML
        await message.answer(f"Вы пригласили {invite_count} пользователей\\.", parse_mode=ParseMode.HTML)
    else:
        await message.answer("Статистика недоступна\\.")

# Обработчик команды /top_ref
@dp.message(Command("top_ref"))
async def cmd_top_ref(message: Message):
    try:
        async with await get_mysql_connection() as conn:
            async with conn.cursor() as cursor:
                # Получаем топ пользователей по количеству приглашений
                await cursor.execute("""
                    SELECT username, invite_count 
                    FROM users 
                    WHERE invite_count > 0 
                    ORDER BY invite_count DESC 
                    LIMIT 10
                """)
                top_users = await cursor.fetchall()

                if not top_users:
                    await message.answer("Пока никто не пригласил рефералов\\.")
                    return

                # Формируем сообщение с топом
                top_message = "🏆 Топ пользователей по количеству приглашений:\n\n"
                for i, user in enumerate(top_users, start=1):
                    top_message += f"{i}\\. {escape_markdown(user['username'])} — {escape_markdown(str(user['invite_count']))} приглашений\n"

                # Отправляем сообщение
                await message.answer(top_message, parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Ошибка при выполнении команды /top_ref: {e}")
        await message.answer("Произошла ошибка при получении данных\\. Пожалуйста, попробуйте позже\\.")


# Обработчик команды /help
@dp.message(Command("help"))
async def cmd_help(message: Message):
    help_text = (
        "Список команд:\n"
        "/ban - Постоянный бан (ответьте на сообщение пользователя)\n"
        "/tempban часы минуты - Временный бан (ответьте на сообщение пользователя)\n"
        "/unban - Разбанить пользователя (ответьте на сообщение пользователя)\n"
        "/mute - Постоянный мут (ответьте на сообщение пользователя)\n"
        "/tempmute часы минуты - Временный мут (ответьте на сообщение пользователя)\n"
        "/unmute - Размутить пользователя (ответьте на сообщение пользователя)\n"
        "/warn - Выдать предупреждение (ответьте на сообщение пользователя)\n"
        "/my_ref - Получить реферальную ссылку\n"
        "/ref_stats - Статистика по рефералам\n"
        "/top_ref — Показать топ пользователей по количеству приглашений\n"
        "/rassilka <текст> — Отправить сообщение всем участникам канала (только для администратора)\n"
    )
    await message.answer(quote_text(help_text))

# Обработчик команды /ban (постоянный бан)
@dp.message(Command("ban"))
async def cmd_ban(message: Message):
    if not message.reply_to_message:
        await message.answer(quote_text("Эта команда должна быть использована в ответ на сообщение пользователя\\."))
        return

    user_to_ban = message.reply_to_message.from_user
    try:
        # Бан пользователя навсегда
        await bot.ban_chat_member(message.chat.id, user_to_ban.id)
        await message.answer(quote_text(f"Пользователь {user_to_ban.full_name} забанен навсегда."))
    except Exception as e:
        await message.answer(quote_text(f"Не удалось забанить пользователя: {e}"))

# Обработчик команды /tempban (временный бан)
@dp.message(Command("tempban"))
async def cmd_tempban(message: Message, command: CommandObject):
    if not message.reply_to_message:
        await message.answer(quote_text("Эта команда должна быть использована в ответ на сообщение пользователя\\."))
        return

    user_to_ban = message.reply_to_message.from_user
    try:
        # Парсим время бана (часы и минуты)
        args = command.args.split() if command.args else []
        if len(args) < 2:
            await message.answer(quote_text("Укажите время бана в формате: /tempban часы минуты"))
            return

        hours = int(args[0])
        minutes = int(args[1])
        until_date = datetime.now() + timedelta(hours=hours, minutes=minutes)

        # Временный бан пользователя
        await bot.ban_chat_member(message.chat.id, user_to_ban.id, until_date=until_date)
        await message.answer(quote_text(f"Пользователь {user_to_ban.full_name} забанен на {hours} часов и {minutes} минут."))
    except Exception as e:
        await message.answer(quote_text(f"Не удалось забанить пользователя: {e}"))

# Обработчик команды /unban
@dp.message(Command("unban"))
async def cmd_unban(message: Message, command: CommandObject):
    if not message.reply_to_message:
        await message.answer(quote_text("Эта команда должна быть использована в ответ на сообщение пользователя\\."))
        return

    user_to_unban = message.reply_to_message.from_user
    try:
        await bot.unban_chat_member(message.chat.id, user_to_unban.id)
        await message.answer(quote_text(f"Пользователь {user_to_unban.full_name} разбанен."))
    except Exception as e:
        await message.answer(quote_text(f"Не удалось разбанить пользователя: {e}"))

# Обработчик команды /mute (постоянный мут)
@dp.message(Command("mute"))
async def cmd_mute(message: Message):
    if not message.reply_to_message:
        await message.answer(quote_text("Эта команда должна быть использована в ответ на сообщение пользователя\\."))
        return

    user_to_mute = message.reply_to_message.from_user
    try:
        # Мут пользователя навсегда
        await bot.restrict_chat_member(message.chat.id, user_to_mute.id, MUTE_PERMISSIONS)
        await message.answer(quote_text(f"Пользователь {user_to_mute.full_name} замучен навсегда."))
    except Exception as e:
        await message.answer(quote_text(f"Не удалось замутить пользователя: {e}"))

# Обработчик команды /tempmute (временный мут)
@dp.message(Command("tempmute"))
async def cmd_tempmute(message: Message, command: CommandObject):
    if not message.reply_to_message:
        await message.answer(quote_text("Эта команда должна быть использована в ответ на сообщение пользователя\\."))
        return

    user_to_mute = message.reply_to_message.from_user
    try:
        # Парсим время мута (часы и минуты)
        args = command.args.split() if command.args else []
        if len(args) < 2:
            await message.answer(quote_text("Укажите время мута в формате: /tempmute часы минуты"))
            return

        hours = int(args[0])
        minutes = int(args[1])
        until_date = datetime.now() + timedelta(hours=hours, minutes=minutes)

        # Временный мут пользователя
        await bot.restrict_chat_member(message.chat.id, user_to_mute.id, MUTE_PERMISSIONS, until_date=until_date)
        await message.answer(quote_text(f"Пользователь {user_to_mute.full_name} замучен на {hours} часов и {minutes} минут."))
    except Exception as e:
        await message.answer(quote_text(f"Не удалось замутить пользователя: {e}"))

# Обработчик команды /unmute
@dp.message(Command("unmute"))
async def cmd_unmute(message: Message):
    if not message.reply_to_message:
        await message.answer(quote_text("Эта команда должна быть использована в ответ на сообщение пользователя\\."))
        return

    user_to_unmute = message.reply_to_message.from_user
    try:
        await bot.restrict_chat_member(message.chat.id, user_to_unmute.id, UNMUTE_PERMISSIONS)
        await message.answer(quote_text(f"Пользователь {user_to_unmute.full_name} размучен."))
    except Exception as e:
        await message.answer(quote_text(f"Не удалось размутить пользователя: {e}"))

# Антиспам: мут при отправке более 3 сообщений за 2 секунды
@dp.message()
async def anti_spam(message: Message):
    # Проверяем, является ли пользователь администратором
    chat_member = await bot.get_chat_member(message.chat.id, message.from_user.id)
    if chat_member.status in ["creator", "administrator"]:
        return  # Если админ, пропускаем антиспам

    user_id = message.from_user.id
    current_time = datetime.now()

    # Сброс счетчика, если прошло больше 2 секунд с последнего сообщения
    if (current_time - user_last_message_time[user_id]).total_seconds() > 2:
        user_message_count[user_id] = 0

    user_message_count[user_id] += 1
    user_last_message_time[user_id] = current_time

    # Если отправлено более 3 сообщений за 2 секунды
    if user_message_count[user_id] > 3:
        try:
            await bot.restrict_chat_member(message.chat.id, user_id, MUTE_PERMISSIONS, until_date=datetime.now() + timedelta(minutes=5))
            await message.answer(quote_text(f"Пользователь {message.from_user.full_name} замучен на 5 минут за спам."))
            user_message_count[user_id] = 0  # Сбрасываем счетчик сообщений
        except Exception as e:
            await message.answer(quote_text(f"Не удалось замутить пользователя: {e}"))

# Обработчик всех сообщений
@dp.message()
async def handle_message(message: Message):
    # Проверяем, содержит ли текст сообщения ключевые слова для игнорирования
    if message.text and any(keyword in message.text for keyword in IGNORE_KEYWORDS):
        print(f"Сообщение с темой 'Чатик' проигнорировано")
        return

    # Получаем данные пользователя
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.full_name

    # Создаем таблицу users_list, если её нет
    await create_users_table()

    # Проверяем, есть ли пользователь в таблице users_list
    async with await get_mysql_connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT user_id FROM users_list WHERE user_id = %s", (user_id,))
            user_exists = await cursor.fetchone()

            # Если пользователя нет в таблице, добавляем его
            if not user_exists:
                await cursor.execute(
                    "INSERT INTO users_list (user_id, username) VALUES (%s, %s) ON DUPLICATE KEY UPDATE username = %s",
                    (user_id, username, username)
                )
                await conn.commit()
                print(f"Пользователь {user_id} ({username}) добавлен в users_list")
                logger.info(f"Пользователь {user_id} ({username}) добавлен в users_list")
 

# Обработчик участника, который покинул чат
@dp.chat_member(ChatMemberUpdatedFilter(MEMBER >> LEFT))
async def goodbye_member(event: ChatMemberUpdated):
    user = event.old_chat_member.user
    # Используем MarkdownV2 для создания ссылки на пользователя и цитаты
    await event.answer(
        f"> [{escape_markdown(user.full_name)}](tg://user?id={user.id}) покинул нас\\. 😢",
        parse_mode=ParseMode.MARKDOWN_V2
    )

# Обработчик участника, который был исключен
@dp.chat_member(ChatMemberUpdatedFilter(MEMBER >> KICKED))
async def kicked_member(event: ChatMemberUpdated):
    user = event.old_chat_member.user
    # Используем MarkdownV2 для создания ссылки на пользователя и цитаты
    await event.answer(
        f"> [{escape_markdown(user.full_name)}](tg://user?id={user.id}) был исключен из чата\\. 🚫",
        parse_mode=ParseMode.MARKDOWN_V2
    )

class TextFilter(BaseFilter):
    def __init__(self, text: str):
        self.text = text.lower()

    async def __call__(self, message: Message) -> bool:
        return self.text in message.text.lower()

# Функция для проверки подключения к группе ВК
async def check_vk_connection():
    try:
        vk.wall.get(owner_id=f"-{VK_GROUP_ID}", count=1)
        logger.info("Успешное подключение к группе ВК")
        return True
    except ApiError as e:
        logger.error(f"Ошибка подключения к группе ВК: {e}")
        return False

# Запуск бота
async def main():
    await on_startup(dp)
    await dp.start_polling(bot)

# Запуск бота
if __name__ == "__main__":
    asyncio.run(main())

app = FastAPI()

@app.post("/ref")
async def handle_referral(request: Request):
    data = await request.json()
    user_id = data.get("user_id")
    referral_code = data.get("code")

    # Логируем переход по ссылке
    logger.info(f"Пользователь {user_id} перешёл по ссылке с реферальным кодом {referral_code}")

    # Сохраняем информацию о переходе в базу данных
    async with await get_mysql_connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                INSERT INTO users (user_id, referral_code)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE referral_code = VALUES(referral_code)
            """, (user_id, referral_code))
            await conn.commit()

    return {"status": "ok"}

# Генерация реферальной ссылки
def generate_referral_link(user_id: int) -> str:
    referral_code = f"REF{user_id}"
    return f"https://yourdomain.com/ref?code={referral_code}"
