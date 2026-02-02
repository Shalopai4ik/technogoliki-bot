import os
import logging
import sys
import io
import asyncio
import time
import aiohttp
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

# Настройка кодировки
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')


# =========== КОНФИГУРАЦИЯ ДЛЯ RENDER.COM ===========

def load_config():
    """Загрузка конфигурации из переменных окружения или файла"""

    # Проверяем, работаем ли мы на Render или Railway
    IS_CLOUD = os.getenv('RENDER') == 'true' or os.getenv('RAILWAY_STATIC_URL') is not None

    if IS_CLOUD:
        # Используем переменные окружения для облачного хостинга
        logger.info("🌐 Обнаружен облачный хостинг, использую переменные окружения")

        API_TOKEN = os.getenv('API_TOKEN')
        MYSQL_HOST = os.getenv('MYSQL_HOST', 'fvh1.spaceweb.ru')
        MYSQL_PORT = int(os.getenv('MYSQL_PORT', '3306'))
        MYSQL_USER = os.getenv('MYSQL_USER', 'hlebopash2')
        MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
        MYSQL_DB = os.getenv('MYSQL_DB', 'hlebopash2')
        VK_ACCESS_TOKEN = os.getenv('VK_ACCESS_TOKEN', '')
        VK_GROUP_ID = os.getenv('VK_GROUP_ID', '229287670')
        CHANNEL_ID = os.getenv('CHANNEL_ID', '@technogoliki')
        CHANNEL_USERNAME = os.getenv('CHANNEL_USERNAME', 'technogoliki')

        # Обработка ADMIN_IDS из строки
        admin_ids_str = os.getenv('ADMIN_IDS', '1981956063,994634615,1412137237,5552131367')
        ADMIN_IDS = [int(id.strip()) for id in admin_ids_str.split(',') if id.strip()]

        LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
        REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '30'))
        POLLING_TIMEOUT = int(os.getenv('POLLING_TIMEOUT', '10'))

        CONFIG_SOURCE = 'переменные окружения (облачный хостинг)'

        # Проверка обязательных переменных для облака
        missing_vars = []
        for var_name, var_value in [
            ('API_TOKEN', API_TOKEN),
            ('MYSQL_USER', MYSQL_USER),
            ('MYSQL_PASSWORD', MYSQL_PASSWORD),
            ('MYSQL_DB', MYSQL_DB)
        ]:
            if not var_value:
                missing_vars.append(var_name)

        if missing_vars:
            error_msg = f"❌ Отсутствуют обязательные переменные окружения: {', '.join(missing_vars)}\n"
            error_msg += "Добавьте их в настройках Render.com:\n"
            error_msg += "1. Зайдите в Dashboard вашего проекта\n"
            error_msg += "2. Выберите 'Environment'\n"
            error_msg += "3. Добавьте переменные: API_TOKEN, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB\n"
            error_msg += "4. Перезапустите деплой"
            print(error_msg)
            if 'API_TOKEN' in missing_vars:
                print("\n🔑 Как получить API_TOKEN:")
                print("1. Найдите @BotFather в Telegram")
                print("2. Отправьте /newbot")
                print("3. Следуйте инструкциям")
                print("4. Скопируйте полученный токен")
            sys.exit(1)

    else:
        # Локальный запуск - используем config.py
        logger.info("💻 Локальный запуск, использую config.py")

        try:
            import config

            API_TOKEN = config.API_TOKEN
            MYSQL_HOST = config.MYSQL_HOST
            MYSQL_PORT = config.MYSQL_PORT
            MYSQL_USER = config.MYSQL_USER
            MYSQL_PASSWORD = config.MYSQL_PASSWORD
            MYSQL_DB = config.MYSQL_DB
            VK_ACCESS_TOKEN = getattr(config, 'VK_ACCESS_TOKEN', '')
            VK_GROUP_ID = getattr(config, 'VK_GROUP_ID', '229287670')
            CHANNEL_ID = getattr(config, 'CHANNEL_ID', '@technogoliki')
            CHANNEL_USERNAME = getattr(config, 'CHANNEL_USERNAME', 'technogoliki')
            ADMIN_IDS = getattr(config, 'ADMIN_IDS', [1981956063, 994634615, 1412137237, 5552131367])
            LOG_LEVEL = getattr(config, 'LOG_LEVEL', 'INFO')
            REQUEST_TIMEOUT = getattr(config, 'REQUEST_TIMEOUT', 30)
            POLLING_TIMEOUT = getattr(config, 'POLLING_TIMEOUT', 10)

            CONFIG_SOURCE = 'config.py (локальный)'

            # Проверка токена для локального запуска
            if not API_TOKEN or API_TOKEN == 'ВАШ_ТОКЕН_БОТА':
                print("\n" + "=" * 60)
                print("❌ ОШИБКА: Токен бота не установлен!")
                print("=" * 60)
                print("Чтобы получить токен:")
                print("1. Откройте Telegram и найдите @BotFather")
                print("2. Отправьте /newbot")
                print("3. Следуйте инструкциям")
                print("4. Полученный токен добавьте в config.py в API_TOKEN")
                print("=" * 60)
                sys.exit(1)

        except ImportError:
            print("\n" + "=" * 60)
            print("❌ Файл config.py не найден!")
            print("=" * 60)
            print("Создайте файл config.py со следующим содержимым:")
            print("""
API_TOKEN = 'ваш_токен_бота'
MYSQL_HOST = 'fvh1.spaceweb.ru'
MYSQL_PORT = 3306
MYSQL_USER = 'hlebopash2'
MYSQL_PASSWORD = 'ваш_пароль'
MYSQL_DB = 'hlebopash2'
VK_ACCESS_TOKEN = ''
VK_GROUP_ID = '229287670'
CHANNEL_ID = '@technogoliki'
CHANNEL_USERNAME = 'technogoliki'
ADMIN_IDS = [1981956063, 994634615, 1412137237, 5552131367]
LOG_LEVEL = 'INFO'
REQUEST_TIMEOUT = 30
POLLING_TIMEOUT = 10
""")
            print("=" * 60)
            sys.exit(1)
        except AttributeError as e:
            print(f"❌ Ошибка в config.py: {e}")
            print("Убедитесь что все переменные определены в config.py")
            sys.exit(1)

    return {
        'API_TOKEN': API_TOKEN,
        'MYSQL_HOST': MYSQL_HOST,
        'MYSQL_PORT': MYSQL_PORT,
        'MYSQL_USER': MYSQL_USER,
        'MYSQL_PASSWORD': MYSQL_PASSWORD,
        'MYSQL_DB': MYSQL_DB,
        'VK_ACCESS_TOKEN': VK_ACCESS_TOKEN,
        'VK_GROUP_ID': VK_GROUP_ID,
        'CHANNEL_ID': CHANNEL_ID,
        'CHANNEL_USERNAME': CHANNEL_USERNAME,
        'ADMIN_IDS': ADMIN_IDS,
        'LOG_LEVEL': LOG_LEVEL,
        'REQUEST_TIMEOUT': REQUEST_TIMEOUT,
        'POLLING_TIMEOUT': POLLING_TIMEOUT,
        'CONFIG_SOURCE': CONFIG_SOURCE,
        'IS_CLOUD': IS_CLOUD
    }


# Загружаем конфигурацию
config = load_config()

# Распаковка конфига для удобства
API_TOKEN = config['API_TOKEN']
MYSQL_HOST = config['MYSQL_HOST']
MYSQL_PORT = config['MYSQL_PORT']
MYSQL_USER = config['MYSQL_USER']
MYSQL_PASSWORD = config['MYSQL_PASSWORD']
MYSQL_DB = config['MYSQL_DB']
VK_ACCESS_TOKEN = config['VK_ACCESS_TOKEN']
VK_GROUP_ID = config['VK_GROUP_ID']
CHANNEL_ID = config['CHANNEL_ID']
CHANNEL_USERNAME = config['CHANNEL_USERNAME']
ADMIN_IDS = config['ADMIN_IDS']
LOG_LEVEL = config['LOG_LEVEL']
REQUEST_TIMEOUT = config['REQUEST_TIMEOUT']
POLLING_TIMEOUT = config['POLLING_TIMEOUT']
CONFIG_SOURCE = config['CONFIG_SOURCE']
IS_CLOUD = config['IS_CLOUD']


# =========== ЛОГГИРОВАНИЕ И МОНИТОРИНГ ===========

class BotMonitor:
    """Класс для мониторинга работы бота"""

    def __init__(self):
        self.start_time = time.time()
        self.stats = {
            'messages_received': 0,
            'commands_processed': 0,
            'errors': 0,
            'users_started': 0,
            'posts_suggested': 0,
        }
        self.last_check = time.time()

    def increment(self, stat_name: str):
        """Увеличить счетчик статистики"""
        if stat_name in self.stats:
            self.stats[stat_name] += 1

    def get_uptime(self) -> str:
        """Получить время работы бота"""
        uptime = time.time() - self.start_time
        hours = int(uptime // 3600)
        minutes = int((uptime % 3600) // 60)
        seconds = int(uptime % 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def get_stats(self) -> Dict[str, Any]:
        """Получить всю статистику"""
        return {
            **self.stats,
            'uptime': self.get_uptime(),
            'running_since': datetime.fromtimestamp(self.start_time).strftime('%Y-%m-%d %H:%M:%S')
        }

    def log_stats(self):
        """Логировать статистику"""
        stats = self.get_stats()
        logger.info("📊 Статистика бота:")
        logger.info(f"   Время работы: {stats['uptime']}")
        logger.info(f"   Сообщений получено: {stats['messages_received']}")
        logger.info(f"   Команд обработано: {stats['commands_processed']}")
        logger.info(f"   Пользователей запустило бота: {stats['users_started']}")
        logger.info(f"   Постов предложено: {stats['posts_suggested']}")
        logger.info(f"   Ошибок: {stats['errors']}")


# Настройка логирования
log_levels = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}

# Для Render логируем только в консоль
if IS_CLOUD:
    logging.basicConfig(
        level=log_levels.get(LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
else:
    logging.basicConfig(
        level=log_levels.get(LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("bot.log", encoding='utf-8', mode='a'),
            logging.StreamHandler(sys.stdout)
        ]
    )

logger = logging.getLogger(__name__)

# Инициализация мониторинга
monitor = BotMonitor()

# =========== ИМПОРТ БИБЛИОТЕК ===========

try:
    import aiomysql

    logger.info("✅ aiomysql загружен")
except ImportError:
    logger.error("❌ aiomysql не установлен. Установите: pip install aiomysql")
    sys.exit(1)

try:
    from aiogram import Bot, Dispatcher, types, F
    from aiogram.filters import Command, CommandObject
    from aiogram.enums import ParseMode, ContentType
    from aiogram.client.default import DefaultBotProperties
    from aiogram.types import (
        Message, KeyboardButton, ReplyKeyboardMarkup,
        InlineKeyboardMarkup, InlineKeyboardButton,
        ReplyKeyboardRemove, FSInputFile
    )

    logger.info("✅ aiogram загружен")
except ImportError:
    logger.error("❌ aiogram не установлен. Установите: pip install aiogram")
    sys.exit(1)

try:
    from vk_api import VkApi
    from vk_api.upload import VkUpload

    VK_AVAILABLE = True
    logger.info("✅ vk-api загружен")
except ImportError:
    VK_AVAILABLE = False
    logger.warning("⚠️ vk-api не установлен. VK функции отключены. Установите: pip install vk-api")

# =========== ИНИЦИАЛИЗАЦИЯ ===========

# Инициализация бота с улучшенными параметрами
bot = Bot(
    token=API_TOKEN,
    default=DefaultBotProperties(
        parse_mode=ParseMode.MARKDOWN_V2,
        link_preview_is_disabled=True
    )
)

dp = Dispatcher()

# Инициализация VK (если доступно)
vk = None
vk_upload = None
if VK_AVAILABLE and VK_ACCESS_TOKEN:
    try:
        vk_session = VkApi(token=VK_ACCESS_TOKEN)
        vk = vk_session.get_api()
        vk_upload = VkUpload(vk_session)
        logger.info("✅ VK API инициализирован")
    except Exception as e:
        logger.warning(f"⚠️ Ошибка инициализации VK API: {e}")
        vk = None
        vk_upload = None
else:
    logger.info("ℹ️ VK API отключен (токен не установлен или модуль недоступен)")

# =========== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ===========

# Текущая тема для предложки
current_topic: Optional[str] = None
user_states: Dict[int, str] = {}  # Состояния пользователей

# Папки для постов (только если не в облаке)
if not IS_CLOUD:
    TOPICS = ["Нейросети", "Программирование", "Разработка игр", "Технологии"]
    for topic in TOPICS:
        os.makedirs(topic, exist_ok=True)
else:
    logger.info("☁️  Облачный режим: сохранение постов в файлы отключено")


# =========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ===========

def escape_markdown(text: str) -> str:
    """Экранирование Markdown символов"""
    reserved_chars = r"\_*[]()~`>#+-=|{}.!"
    for char in reserved_chars:
        text = text.replace(char, f"\\{char}")
    return text


def quote_text(text: str) -> str:
    """Оформление текста как цитаты"""
    return f"> {escape_markdown(text)}"


async def get_mysql_connection():
    """Подключение к MySQL"""
    try:
        connection = await aiomysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            db=MYSQL_DB,
            cursorclass=aiomysql.DictCursor,
            connect_timeout=10
        )
        return connection
    except Exception as e:
        logger.error(f"Ошибка подключения к MySQL: {e}")
        logger.error(f"Подробности: host={MYSQL_HOST}, user={MYSQL_USER}, db={MYSQL_DB}")
        monitor.increment('errors')
        raise


async def create_tables_if_not_exists():
    """Создание таблиц если их нет"""
    try:
        async with await get_mysql_connection() as conn:
            async with conn.cursor() as cursor:
                # Отключаем предупреждения
                await cursor.execute("SET sql_notes = 0;")

                # Таблица users
                await cursor.execute("""
                                     CREATE TABLE IF NOT EXISTS users
                                     (
                                         id
                                         INT
                                         AUTO_INCREMENT
                                         PRIMARY
                                         KEY,
                                         user_id
                                         BIGINT
                                         NOT
                                         NULL
                                         UNIQUE,
                                         username
                                         VARCHAR
                                     (
                                         255
                                     ),
                                         referral_code VARCHAR
                                     (
                                         50
                                     ) UNIQUE,
                                         invited_by BIGINT,
                                         invite_count INT DEFAULT 0,
                                         referral_link VARCHAR
                                     (
                                         255
                                     ),
                                         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                                         )
                                     """)

                # Таблица users_list
                await cursor.execute("""
                                     CREATE TABLE IF NOT EXISTS users_list
                                     (
                                         user_id
                                         BIGINT
                                         PRIMARY
                                         KEY,
                                         username
                                         VARCHAR
                                     (
                                         255
                                     ),
                                         last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                                         )
                                     """)

                # Таблица posts
                await cursor.execute("""
                                     CREATE TABLE IF NOT EXISTS posts
                                     (
                                         id
                                         INT
                                         AUTO_INCREMENT
                                         PRIMARY
                                         KEY,
                                         user_id
                                         BIGINT,
                                         topic
                                         VARCHAR
                                     (
                                         50
                                     ),
                                         status VARCHAR
                                     (
                                         20
                                     ) DEFAULT 'pending',
                                         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                                         )
                                     """)

                # Включаем предупреждения
                await cursor.execute("SET sql_notes = 1;")
                await conn.commit()

        logger.info("✅ Таблицы проверены/созданы")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка при создании таблиц: {e}")
        monitor.increment('errors')
        return False


# =========== КЛАВИАТУРЫ ===========

def get_main_keyboard():
    """Главная клавиатура"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📰 Предложить новость")],
            [KeyboardButton(text="❓ Задать вопрос")]
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие..."
    )


def get_topics_keyboard():
    """Клавиатура с темами"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🤖 Нейросети"), KeyboardButton(text="💻 Программирование")],
            [KeyboardButton(text="🎮 Разработка игр"), KeyboardButton(text="⚙️ Технологии")],
            [KeyboardButton(text="⬅️ Назад в главное меню")]
        ],
        resize_keyboard=True
    )


def get_questions_keyboard():
    """Клавиатура с вопросами"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🤖 Что ты такое?")],
            [KeyboardButton(text="💻 Как предложить пост?")],
            [KeyboardButton(text="🎮 Как попасть в команду?")],
            [KeyboardButton(text="⚙️ Ссылки на ресурсы")],
            [KeyboardButton(text="⬅️ Назад в главное меню")]
        ],
        resize_keyboard=True
    )


def get_back_only_keyboard():
    """Только кнопка назад"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="⬅️ Назад в главное меню")]
        ],
        resize_keyboard=True
    )


# =========== ОБРАБОТЧИКИ КОМАНД ===========

@dp.message(Command("start"))
async def cmd_start(message: Message, command: CommandObject):
    """Обработчик команды /start"""
    try:
        monitor.increment('messages_received')
        monitor.increment('commands_processed')
        monitor.increment('users_started')

        user_id = message.from_user.id
        username = message.from_user.full_name

        logger.info(f"👤 Пользователь {user_id} ({username}) запустил бота")

        # Регистрация пользователя
        try:
            async with await get_mysql_connection() as conn:
                async with conn.cursor() as cursor:
                    # Добавляем/обновляем в users_list
                    await cursor.execute(
                        """INSERT INTO users_list (user_id, username)
                           VALUES (%s, %s) ON DUPLICATE KEY
                        UPDATE username = %s, last_seen = CURRENT_TIMESTAMP""",
                        (user_id, username, username)
                    )

                    # Обработка реферального кода
                    referral_code = command.args
                    if referral_code and referral_code.startswith("REF"):
                        try:
                            invited_by = int(referral_code.replace("REF", ""))
                            current_referral_code = f"REF{user_id}"
                            current_referral_link = f"https://t.me/technogoliki_IT_bot?start={current_referral_code}"

                            await cursor.execute(
                                """INSERT INTO users (user_id, username, referral_code, invited_by, referral_link)
                                   VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY
                                UPDATE username = %s, referral_link = %s""",
                                (user_id, username, current_referral_code, invited_by,
                                 current_referral_link, username, current_referral_link)
                            )

                            # Обновляем счетчик приглашений
                            await cursor.execute(
                                "UPDATE users SET invite_count = invite_count + 1 WHERE user_id = %s",
                                (invited_by,)
                            )
                        except ValueError:
                            pass

                    await conn.commit()
        except Exception as e:
            logger.error(f"Ошибка при регистрации пользователя: {e}")
            monitor.increment('errors')

        # Приветственное сообщение
        welcome_text = (
            f"👋 Привет, {escape_markdown(username)}\\!\\!\n\n"
            "Я бот канала *Technogoliki* 🤖\n\n"
            "✨ *Что я умею:*\n"
            "• Принимать предложки постов 📝\n"
            "• Отвечать на частые вопросы ❓\n"
            "• Работать с реферальной системой 🔗\n\n"
            "Выберите действие в меню ниже ⬇️"
        )

        await message.answer(
            welcome_text,
            reply_markup=get_main_keyboard(),
            parse_mode=ParseMode.MARKDOWN_V2
        )

    except Exception as e:
        logger.error(f"Ошибка в команде /start: {e}")
        monitor.increment('errors')
        await message.answer(
            "Произошла ошибка. Пожалуйста, попробуйте позже.",
            reply_markup=get_main_keyboard()
        )


@dp.message(Command("help"))
async def cmd_help(message: Message):
    """Справка по командам"""
    monitor.increment('messages_received')
    monitor.increment('commands_processed')

    help_text = (
        "📋 *Доступные команды:*\n\n"
        "Для всех пользователей:\n"
        "`/start` \\- Начать работу с ботом\n"
        "`/help` \\- Эта справка\n"
        "`/my_ref` \\- Моя реферальная ссылка\n"
        "`/ref_stats` \\- Моя статистика\n"
        "`/top_ref` \\- Топ рефереров\n\n"
        "Для администраторов:\n"
        "`/status` \\- Статус бота\n"
        "`/stats` \\- Статистика\n"
        "`/rassilka` \\- Рассылка\n"
        "`/predlozhka` \\- Просмотр предложенных постов\n"
        "`/clean` \\- Очистка временных данных\n"
    )

    await message.answer(
        help_text,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_main_keyboard()
    )


# =========== ГЛАВНОЕ МЕНЮ ===========

@dp.message(F.text == "📰 Предложить новость")
async def suggest_news(message: Message):
    """Кнопка 'Предложить новость'"""
    monitor.increment('messages_received')

    user_id = message.from_user.id
    user_states[user_id] = 'suggesting_topic'

    await message.answer(
        "🎯 *Выберите тему для вашей новости:*\n\n"
        "• 🤖 *Нейросети* \\- ИИ, нейросети, машинное обучение\n"
        "• 💻 *Программирование* \\- Код, разработка, IT\n"
        "• 🎮 *Разработка игр* \\- Геймдев, игровые движки\n"
        "• ⚙️ *Технологии* \\- Гаджеты, инновации, наука",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_topics_keyboard()
    )


@dp.message(F.text == "❓ Задать вопрос")
async def ask_question(message: Message):
    """Кнопка 'Задать вопрос'"""
    monitor.increment('messages_received')

    await message.answer(
        "❓ *Выберите интересующий вопрос:*",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_questions_keyboard()
    )


@dp.message(F.text == "⬅️ Назад в главное меню")
async def back_to_main(message: Message):
    """Кнопка 'Назад в главное меню'"""
    monitor.increment('messages_received')

    user_id = message.from_user.id
    if user_id in user_states:
        del user_states[user_id]

    await message.answer(
        "🏠 *Главное меню*",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_main_keyboard()
    )


# =========== ВЫБОР ТЕМЫ ДЛЯ ПОСТА ===========

@dp.message(F.text.in_(["🤖 Нейросети", "💻 Программирование", "🎮 Разработка игр", "⚙️ Технологии"]))
async def select_topic(message: Message):
    """Выбор темы для поста"""
    monitor.increment('messages_received')
    monitor.increment('posts_suggested')

    user_id = message.from_user.id
    topic_map = {
        "🤖 Нейросети": "Нейросети",
        "💻 Программирование": "Программирование",
        "🎮 Разработка игр": "Разработка игр",
        "⚙️ Технологии": "Технологии"
    }

    topic = topic_map[message.text]
    global current_topic
    current_topic = topic
    user_states[user_id] = f'waiting_post_{topic}'

    # Сохраняем в БД
    try:
        async with await get_mysql_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """INSERT INTO posts (user_id, topic, status)
                       VALUES (%s, %s, 'pending')""",
                    (user_id, topic)
                )
                await conn.commit()
    except Exception as e:
        logger.error(f"Ошибка сохранения поста в БД: {e}")

    instruction = (
        f"✅ Тема выбрана: *{topic}*\n\n"
        "📝 *Инструкция по отправке поста:*\n"
        "1. Прикрепите *одно* фото 📷\n"
        "2. Добавьте описание в подписи\n"
        "3. Отправьте одним сообщением\n\n"
        "⚠️ *Важно:*\n"
        "• Если прикрепите несколько фото, будет использовано *только первое*\n"
        "• Описание обязательно\n"
        "• Пост будет отправлен администраторам на модерацию\n\n"
        "Или нажмите '⬅️ Назад в главное меню' для отмены"
    )

    await message.answer(
        instruction,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_back_only_keyboard()
    )


# =========== ПРИЕМ ПОСТОВ С ФОТО ===========

@dp.message(F.content_type == ContentType.PHOTO)
async def process_post_with_photo(message: Message):
    """Обработка поста с фото"""
    monitor.increment('messages_received')

    user_id = message.from_user.id
    username = message.from_user.full_name

    # Проверяем, ожидаем ли мы пост от этого пользователя
    if user_id not in user_states or not user_states[user_id].startswith('waiting_post_'):
        await message.answer(
            "Сначала выберите тему для поста через меню '📰 Предложить новость'",
            reply_markup=get_main_keyboard()
        )
        return

    global current_topic
    if not current_topic:
        await message.answer(
            "Тема не выбрана. Вернитесь в меню и выберите тему.",
            reply_markup=get_main_keyboard()
        )
        return

    if not message.caption:
        await message.answer(
            "⚠️ Пожалуйста, добавьте описание к фото!\n"
            "Отправьте фото с текстом в подписи.",
            reply_markup=get_back_only_keyboard()
        )
        return

    try:
        # В облачном режиме не сохраняем файлы
        if IS_CLOUD:
            caption = message.caption
            hashtag = f"#{current_topic.lower().replace(' ', '_')}"

            # Просто логируем и уведомляем
            logger.info(f"📸 Пользователь {user_id} предложил пост в тему '{current_topic}'")

            success_message = (
                f"✅ *Пост успешно принят\\!*\n\n"
                f"📁 Тема: *{current_topic}*\n"
                f"📝 Описание: {len(caption)} символов\n\n"
                f"Ваш пост отправлен на модерацию администраторам\\. "
                f"Спасибо за предложку\\! 🙏\n\n"
                f"Хэштег: {hashtag}\n"
                f"ℹ️ *В облачном режиме фото не сохраняется*"
            )
        else:
            # Локальный режим - сохраняем файлы
            topic_folder = current_topic
            images_folder = os.path.join(topic_folder, "Картинки")
            content_folder = os.path.join(topic_folder, "Содержимое")

            os.makedirs(images_folder, exist_ok=True)
            os.makedirs(content_folder, exist_ok=True)

            # Ищем следующий номер
            existing_images = [f for f in os.listdir(images_folder) if f.endswith('.jpg')]
            existing_numbers = [int(f.split('.')[0]) for f in existing_images if f.split('.')[0].isdigit()]
            next_number = max(existing_numbers) + 1 if existing_numbers else 1

            # Сохраняем фото
            photo = message.photo[-1]
            photo_path = os.path.join(images_folder, f"{next_number:04d}.jpg")
            await bot.download(photo, destination=photo_path)

            # Сохраняем текст
            caption = message.caption
            hashtag = f"#{current_topic.lower().replace(' ', '_')}"
            text_with_hashtag = f"{caption}\n\n{hashtag}"

            text_path = os.path.join(content_folder, f"{next_number:04d}.txt")
            with open(text_path, 'w', encoding='utf-8') as f:
                f.write(text_with_hashtag)

            success_message = (
                f"✅ *Пост успешно сохранён\\!*\n\n"
                f"📁 Тема: *{current_topic}*\n"
                f"📷 Фото: сохранено\n"
                f"📝 Описание: {len(caption)} символов\n"
                f"🔢 Номер: #{next_number:04d}\n\n"
                f"Ваш пост отправлен на модерацию администраторам\\. "
                f"Спасибо за предложку\\! 🙏\n\n"
                f"Хэштег: {hashtag}"
            )

            logger.info(f"📸 Пользователь {user_id} предложил пост в тему '{current_topic}' (№{next_number})")

        # Обновляем состояние
        if user_id in user_states:
            del user_states[user_id]

        # Обновляем статус в БД
        try:
            async with await get_mysql_connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        """UPDATE posts
                           SET status = 'submitted'
                           WHERE user_id = %s
                             AND topic = %s
                             AND status = 'pending' ORDER BY created_at DESC LIMIT 1""",
                        (user_id, current_topic)
                    )
                    await conn.commit()
        except Exception as e:
            logger.error(f"Ошибка обновления статуса поста: {e}")

        await message.answer(
            success_message,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=get_main_keyboard()
        )

        # Уведомление админам
        admin_notification = (
            f"📨 *Новый пост предложен\\!*\n\n"
            f"👤 Пользователь: {escape_markdown(username)} \\(ID: {user_id}\\)\n"
            f"🏷️ Тема: *{current_topic}*\n"
            f"📝 Длина описания: {len(caption)} символов\n"
        )

        if not IS_CLOUD:
            admin_notification += f"🔢 Номер: #{next_number:04d}"
        else:
            admin_notification += "☁️ *Облачный режим*"

        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(
                    chat_id=admin_id,
                    text=admin_notification,
                    parse_mode=ParseMode.MARKDOWN_V2
                )
            except Exception as e:
                logger.error(f"Не удалось уведомить админа {admin_id}: {e}")

        # Сбрасываем глобальную тему
        current_topic = None

    except Exception as e:
        logger.error(f"Ошибка обработки поста: {e}")
        monitor.increment('errors')
        await message.answer(
            "❌ Произошла ошибка при сохранении поста. Попробуйте ещё раз.",
            reply_markup=get_main_keyboard()
        )


# =========== ОТВЕТЫ НА ВОПРОСЫ ===========

@dp.message(F.text == "🤖 Что ты такое?")
async def what_is_bot(message: Message):
    """Информация о боте"""
    monitor.increment('messages_received')

    response = (
        "🤖 *Что это за бот?*\n\n"
        "Я основной бот канала *Technogoliki* 🚀\n\n"
        "✨ *Мои задачи:*\n"
        "1️⃣ Поддержка реферальной системы 🔗\n"
        "2️⃣ Приём предложенных постов 📝\n"
        "3️⃣ Ответы на частые вопросы ❓\n"
        "4️⃣ Модерация и уведомления 👮\n\n"
        "📊 *Статистика бота:*\n"
        f"• Время работы: {monitor.get_uptime()}\n"
        f"• Сообщений обработано: {monitor.stats['messages_received']}\n"
        f"• Пользователей: {monitor.stats['users_started']}\n\n"
        "Бот разработан специально для нашего канала\\!"
    )

    await message.answer(
        response,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_questions_keyboard()
    )


@dp.message(F.text == "💻 Как предложить пост?")
async def how_to_suggest_post(message: Message):
    """Как предложить пост"""
    monitor.increment('messages_received')

    response = (
        "📌 *Как предложить пост?*\n\n"
        "1️⃣ Нажмите '📰 Предложить новость'\n"
        "2️⃣ Выберите тему из 4 вариантов 🏷️\n"
        "3️⃣ Прикрепите *одно* фото 📷\n"
        "4️⃣ Добавьте описание в подписи ✍️\n"
        "5️⃣ Отправьте одним сообщением 📨\n\n"
        "⚠️ *Важно:*\n"
        "• Если несколько фото \\- берётся *первое*\n"
        "• Описание *обязательно*\n"
        "• Пост проходит модерацию ✅\n"
        "• Можно отменить через 'Назад'\n\n"
        "🎯 *Темы:* Нейросети, Программирование, Разработка игр, Технологии"
    )

    await message.answer(
        response,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_questions_keyboard()
    )


@dp.message(F.text == "🎮 Как попасть в команду?")
async def how_to_join_team(message: Message):
    """Как попасть в команду"""
    monitor.increment('messages_received')

    response = (
        "🎮 *Как попасть в команду?*\n\n"
        "🚀 Ищем активных авторов и модераторов\\!\n\n"
        "📋 *Требования:*\n"
        "• Знание тематики канала 🧠\n"
        "• Умение писать интересные посты ✍️\n"
        "• Активность и ответственность ⏰\n"
        "• Желание развиваться 📈\n\n"
        "👥 *Контакты администраторов:*\n"
        "• *Dmk\\_*: https://t\\.me/dmk\\_nya\n"
        "• *SirAndriy*: https://t\\.me/SirAndriy\n"
        "• *MARGO*: https://t\\.me/AmiiigoooX\n"
        "• *Anikey*: https://t\\.me/anikey20\n\n"
        "Напишите любому из нас с пометкой 'В команду'\\!"
    )

    await message.answer(
        response,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_questions_keyboard()
    )


@dp.message(F.text == "⚙️ Ссылки на ресурсы")
async def external_links(message: Message):
    """Ссылки на ресурсы"""
    monitor.increment('messages_received')

    response = (
        "🌐 *Ссылки на наши ресурсы:*\n\n"
        "📺 *RUTUBE*: https://rutube\\.ru/channel/25582668\n"
        "🌍 *NUUM*: https://nuum\\.ru/channel/technogoliki\n"
        "📰 *DTF*: https://dtf\\.ru/u/2463675\\-technogoliki\n"
        "🎵 *TIKTOK*: https://www\\.tiktok\\.com/@technogoliki\n"
        "📸 *INSTAGRAM*: https://www\\.instagram\\.com/technogolik\n"
        "🧵 *THREADS*: https://www\\.threads\\.net/@technogolik\n"
        "💎 *BOOSTY*: https://boosty\\.to/technogoliki\n\n"
        "📢 *Telegram канал:* https://t\\.me/technogoliki"
    )

    await message.answer(
        response,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_questions_keyboard()
    )


# =========== АДМИН КОМАНДЫ ===========

@dp.message(Command("status"))
async def cmd_status(message: Message):
    """Статус бота (админ)"""
    if message.from_user.id not in ADMIN_IDS:
        return

    monitor.increment('messages_received')
    monitor.increment('commands_processed')

    # Информация о системе
    system_info = ""
    try:
        import psutil
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')

        system_info = (
            f"• CPU: {cpu_percent}%\n"
            f"• Память: {memory.percent}% ({memory.used // (1024 ** 2)}/{memory.total // (1024 ** 2)} MB)\n"
            f"• Диск: {disk.percent}% ({disk.used // (1024 ** 3)}/{disk.total // (1024 ** 3)} GB)\n\n"
        )
    except ImportError:
        system_info = "• psutil не установлен для мониторинга системы\n\n"
    except Exception as e:
        system_info = f"• Ошибка мониторинга системы: {e}\n\n"

    # Информация о боте
    try:
        bot_info = await bot.get_me()
        bot_username = bot_info.username
    except:
        bot_username = "Не доступно"

    status_text = (
            "🖥️ *Статус системы:*\n" +
            system_info +
            "🤖 *Статус бота:*\n"
            f"• Имя: @{bot_username}\n"
            f"• Время работы: {monitor.get_uptime()}\n"
            f"• Запущен: {datetime.fromtimestamp(monitor.start_time).strftime('%d.%m.%Y %H:%M:%S')}\n"
            f"• Режим: {'☁️ Облачный' if IS_CLOUD else '💻 Локальный'}\n\n"

            "📊 *Статистика:*\n"
            f"• Сообщений: {monitor.stats['messages_received']}\n"
            f"• Команд: {monitor.stats['commands_processed']}\n"
            f"• Пользователей: {monitor.stats['users_started']}\n"
            f"• Постов: {monitor.stats['posts_suggested']}\n"
            f"• Ошибок: {monitor.stats['errors']}\n\n"

            "🗄️ *База данных:*\n"
            f"• Хост: {MYSQL_HOST}\n"
            f"• База: {MYSQL_DB}\n"
            f"• Пользователь: {MYSQL_USER}\n\n"

            "🔗 *Интеграции:*\n"
            f"• VK: {'✅' if vk else '❌'}\n"
            f"• Канал: {CHANNEL_USERNAME}\n"
            f"• Админов: {len(ADMIN_IDS)}"
    )

    await message.answer(
        status_text,
        parse_mode=ParseMode.MARKDOWN_V2
    )


@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    """Статистика (админ)"""
    if message.from_user.id not in ADMIN_IDS:
        return

    monitor.increment('messages_received')
    monitor.increment('commands_processed')

    # Получаем статистику из БД
    try:
        async with await get_mysql_connection() as conn:
            async with conn.cursor() as cursor:
                # Общая статистика пользователей
                await cursor.execute("SELECT COUNT(*) as count FROM users_list")
                total_users = (await cursor.fetchone())['count']

                await cursor.execute("SELECT COUNT(*) as count FROM users")
                total_registered = (await cursor.fetchone())['count']

                await cursor.execute("SELECT COUNT(*) as count FROM posts WHERE status = 'submitted'")
                total_posts = (await cursor.fetchone())['count']

                # Топ рефереров
                await cursor.execute("""
                                     SELECT username, invite_count
                                     FROM users
                                     WHERE invite_count > 0
                                     ORDER BY invite_count DESC LIMIT 5
                                     """)
                top_refs = await cursor.fetchall()

                # Последние посты
                await cursor.execute("""
                                     SELECT p.topic, u.username, p.created_at
                                     FROM posts p
                                              LEFT JOIN users_list u ON p.user_id = u.user_id
                                     WHERE p.status = 'submitted'
                                     ORDER BY p.created_at DESC LIMIT 5
                                     """)
                recent_posts = await cursor.fetchall()

    except Exception as e:
        logger.error(f"Ошибка получения статистики из БД: {e}")
        total_users = total_registered = total_posts = 0
        top_refs = []
        recent_posts = []

    stats_text = (
        "📈 *Статистика базы данных:*\n\n"

        "👥 *Пользователи:*\n"
        f"• Всего в списке: {total_users}\n"
        f"• Зарегистрировано: {total_registered}\n"
        f"• Постов предложено: {total_posts}\n\n"

        "🏆 *Топ рефереров:*\n"
    )

    if top_refs:
        for i, ref in enumerate(top_refs, 1):
            stats_text += f"{i}. {escape_markdown(ref['username'])} - {ref['invite_count']} приглаш.\n"
    else:
        stats_text += "Нет данных\n"

    stats_text += "\n📨 *Последние посты:*\n"
    if recent_posts:
        for post in recent_posts:
            date = post['created_at'].strftime('%d.%m %H:%M') if isinstance(post['created_at'], datetime) else str(
                post['created_at'])
            stats_text += f"• {post['topic']} от {escape_markdown(post['username'] or 'Аноним')} ({date})\n"
    else:
        stats_text += "Нет постов\n"

    stats_text += f"\n📊 *Время работы бота:* {monitor.get_uptime()}"
    stats_text += f"\n🌐 *Режим:* {'☁️ Облачный' if IS_CLOUD else '💻 Локальный'}"

    await message.answer(
        stats_text,
        parse_mode=ParseMode.MARKDOWN_V2
    )


@dp.message(Command("clean"))
async def cmd_clean(message: Message):
    """Очистка временных данных (админ)"""
    if message.from_user.id not in ADMIN_IDS:
        return

    monitor.increment('messages_received')
    monitor.increment('commands_processed')

    global current_topic
    user_states.clear()
    current_topic = None

    await message.answer(
        "🧹 *Очистка выполнена:*\n"
        "• Сброшены состояния пользователей\n"
        "• Сброшена текущая тема\n"
        "• Кэш очищен",
        parse_mode=ParseMode.MARKDOWN_V2
    )


# =========== РЕФЕРАЛЬНАЯ СИСТЕМА ===========

@dp.message(Command("my_ref"))
async def cmd_my_ref(message: Message):
    """Моя реферальная ссылка"""
    monitor.increment('messages_received')
    monitor.increment('commands_processed')

    user_id = message.from_user.id
    username = message.from_user.full_name

    try:
        async with await get_mysql_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT referral_code, invite_count FROM users WHERE user_id = %s",
                    (user_id,)
                )
                user_data = await cursor.fetchone()

                if not user_data:
                    # Создаем реферальную запись
                    referral_code = f"REF{user_id}"
                    referral_link = f"https://t.me/technogoliki_IT_bot?start={referral_code}"

                    await cursor.execute(
                        """INSERT INTO users (user_id, username, referral_code, invite_count, referral_link)
                           VALUES (%s, %s, %s, 0, %s) ON DUPLICATE KEY
                        UPDATE username = %s""",
                        (user_id, username, referral_code, referral_link, username)
                    )

                    user_data = {'referral_code': referral_code, 'invite_count': 0}
                    await conn.commit()

                # Получаем приглашенных пользователей
                await cursor.execute(
                    "SELECT username FROM users WHERE invited_by = %s",
                    (user_id,)
                )
                invited_users = await cursor.fetchall()

                ref_text = (
                    f"🔗 *Ваша реферальная система*\n\n"
                    f"📋 *Код:* `{user_data['referral_code']}`\n"
                    f"🔗 *Ссылка:* https://t\\.me/technogoliki\\_IT\\_bot\\?start\\={user_data['referral_code']}\n"
                    f"👥 *Приглашено:* {user_data['invite_count']} человек\n\n"
                )

                if invited_users:
                    ref_text += "*Приглашённые:*\n"
                    for i, user in enumerate(invited_users, 1):
                        ref_text += f"{i}. {escape_markdown(user['username'])}\n"
                else:
                    ref_text += "Вы ещё никого не пригласили\\."

                await message.answer(
                    ref_text,
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=get_main_keyboard()
                )

    except Exception as e:
        logger.error(f"Ошибка получения реферальной информации: {e}")
        monitor.increment('errors')
        await message.answer(
            "❌ Ошибка получения реферальной информации.",
            reply_markup=get_main_keyboard()
        )


# =========== ОБРАБОТКА ВСЕХ СООБЩЕНИЙ ===========

@dp.message()
async def handle_all_messages(message: Message):
    """Обработка всех сообщений"""
    monitor.increment('messages_received')

    # Логируем все сообщения
    user_id = message.from_user.id
    username = message.from_user.full_name
    text = message.text or "без текста"

    logger.debug(f"Сообщение от {user_id} ({username}): {text[:100]}...")

    # Если текст не обработан выше, отправляем в главное меню
    if message.text and not message.text.startswith('/'):
        await message.answer(
            "Выберите действие из меню ниже:",
            reply_markup=get_main_keyboard()
        )


# =========== ЗАПУСК БОТА ===========

async def on_startup():
    """Действия при запуске бота"""
    print("\n" + "=" * 60)
    print("🤖 ЗАПУСК ТЕЛЕГРАМ БОТА")
    print("=" * 60)
    print(f"Конфигурация: {CONFIG_SOURCE}")
    print(f"Токен бота: {'✅ Установлен' if API_TOKEN else '❌ НЕ УСТАНОВЛЕН'}")
    print(f"MySQL: {MYSQL_HOST}:{MYSQL_PORT}")
    print(f"VK API: {'✅ Доступен' if vk else '❌ Отключен'}")
    print(f"Админов: {len(ADMIN_IDS)}")
    print(f"Режим: {'☁️ Облачный' if IS_CLOUD else '💻 Локальный'}")
    print("=" * 60)

    logger.info("🚀 Инициализация бота...")

    # Проверка таблиц
    if await create_tables_if_not_exists():
        logger.info("✅ База данных готова")
    else:
        logger.warning("⚠️ Проблемы с базой данных")

    # Проверяем подключение к MySQL
    try:
        async with await get_mysql_connection() as conn:
            await conn.ping()
        logger.info("✅ Подключение к MySQL успешно")
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к MySQL: {e}")
        print(f"❌ Ошибка подключения к MySQL: {e}")
        print("Проверьте:")
        print("1. Доступность MySQL сервера")
        print("2. Правильность логина и пароля")
        print("3. Разрешен ли доступ с IP хостинга")
        if IS_CLOUD:
            print("4. В настройках Render добавлены переменные окружения")
        raise

    # Получаем информацию о боте
    try:
        bot_info = await bot.get_me()
        logger.info(f"✅ Бот авторизован: @{bot_info.username} (ID: {bot_info.id})")
        print(f"\n✅ Бот запущен: @{bot_info.username}")
        print(f"👋 Напишите боту: https://t.me/{bot_info.username}")
        if not IS_CLOUD:
            print("\n📊 Мониторинг запущен. Логи пишутся в bot.log")
        print("=" * 60)
    except Exception as e:
        logger.error(f"❌ Ошибка авторизации бота: {e}")
        print(f"❌ Ошибка авторизации: {e}")
        print("Проверьте API_TOKEN в переменных окружения или config.py")
        raise

    # Планируем периодический лог статистики
    async def periodic_stats():
        while True:
            await asyncio.sleep(7200)  # Каждые 2 часа
            monitor.log_stats()

    # Запускаем в фоне
    asyncio.create_task(periodic_stats())


async def on_shutdown():
    """Действия при остановке бота"""
    logger.info("🛑 Остановка бота...")
    monitor.log_stats()

    # Закрываем соединения
    await bot.session.close()


async def main():
    """Основная функция"""
    try:
        await on_startup()
        await dp.start_polling(
            bot,
            polling_timeout=POLLING_TIMEOUT,
            allowed_updates=dp.resolve_used_update_types()
        )
    except KeyboardInterrupt:
        logger.info("⏹️ Остановка по команде пользователя")
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}", exc_info=True)
    finally:
        await on_shutdown()


if __name__ == "__main__":
    # Проверяем зависимости
    try:
        import psutil

        logger.info("✅ psutil загружен для мониторинга системы")
    except ImportError:
        logger.warning("⚠️ psutil не установлен. Мониторинг системы отключен.")

    # Запускаем бота
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Бот остановлен")
    except Exception as e:
        print(f"\n💥 Ошибка запуска: {e}")
        if IS_CLOUD:
            print("\n🔧 Для Render.com:")
            print("1. Проверьте переменные окружения в Dashboard")
            print("2. Убедитесь что API_TOKEN, MYSQL_PASSWORD установлены")
            print("3. Проверьте логи в Render Dashboard")