# Обработчик команды /start
@dp.message(Command("start"))
async def cmd_start(message: Message, command: CommandObject):
    # Извлекаем реферальный код из параметра start
    referral_code = command.args

    user_id = message.from_user.id
    username = message.from_user.full_name

    if referral_code:
        # Если есть реферальный код, сохраняем информацию о приглашении
        referrer_id = referral_code.replace("REF", "")  # Извлекаем ID пригласившего
        referral_data[user_id] = {"referrer_id": referrer_id, "username": username}

        # Уведомляем пригласившего
        await bot.send_message(
            chat_id=referrer_id,
            text=f"🎉 Пользователь {username} присоединился по вашей ссылке!"
        )

    await message.answer(f"Привет, {username}! Добро пожаловать в бота!")

# Обработчик команды /my_ref
@dp.message(Command("my_ref"))
async def cmd_my_ref(message: Message):
    user_id = message.from_user.id

    # Генерируем реферальную ссылку
    referral_link = f"https://t.me/your_bot?start=REF{user_id}"

    await message.answer(
        f"Ваша реферальная ссылка:\n{referral_link}\n\n"
        "Поделитесь этой ссылкой с друзьями, чтобы получить бонусы!"
    )

# Обработчик команды /ref_stats
@dp.message(Command("ref_stats"))
async def cmd_ref_stats(message: Message):
    user_id = message.from_user.id

    # Считаем количество приглашенных пользователей
    invited_users = [data for data in referral_data.values() if data["referrer_id"] == str(user_id)]

    await message.answer(
        f"Вы пригласили {len(invited_users)} пользователей:\n"
        + "\n".join([f"- {user['username']}" for user in invited_users])
    )