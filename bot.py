import asyncio
import html
import logging
import os
import random
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "game_bot.db"
START_BALANCE = 2500
WORK_REWARD = 500
WORK_COOLDOWN_HOURS = 2
DAILY_REWARD = 900
DAILY_COOLDOWN_HOURS = 24
COIN_NAME = "шестерёнки"
ROULETTE_COLORS = ("red", "black", "green")
MINES_REWARD_PERCENTS = {1: 35, 3: 150, 6: 300}
ADMIN_ID = 0
TRANSLATIONS = {
    "ru": {
        "coin_name": "шестерёнки",
        "menu_profile": "👤 Профиль",
        "menu_top": "🏆 Топ игроков",
        "menu_trade": "📈 Трейд",
        "menu_roulette": "🎡 Рулетка",
        "menu_mines": "💣 Мины",
        "menu_work": "🛠 Работа",
        "menu_daily": "🎁 Ежедневный бонус",
        "menu_transfer": "💸 Перевод",
        "menu_profile_link": "🔗 Ссылка на профиль",
        "my_id": "🆔 Твой ID: <code>{user_id}</code>",
        "admin_only": "⛔ Эта команда доступна только админу.",
        "admin_panel": "🛡 Админ-панель\n\nДоступно: рассылка по всем пользователям бота.",
        "admin_broadcast_button": "📢 Рассылка",
        "admin_broadcast_prompt": "📢 Рассылка\n\nОтправь текст сообщения, которое нужно разослать всем пользователям.",
        "admin_broadcast_cancel": "❌ Отменить",
        "admin_broadcast_cancelled": "❌ Рассылка отменена.",
        "admin_broadcast_running": "📡 Рассылка запущена...",
        "admin_broadcast_done": "📢 Рассылка завершена.\n\nОтправлено: {sent}\nНе доставлено: {failed}",
        "menu_placeholder": "Выбери раздел из меню ниже",
        "trade_up": "📈 вверх",
        "trade_down": "📉 вниз",
        "roulette_red": "🔴 красное",
        "roulette_black": "⚫ чёрное",
        "roulette_green": "🟢 зелёное",
        "profile_title": "🧾 Профиль игрока",
        "profile_name": "🪪 Имя",
        "profile_balance": "💳 Баланс",
        "profile_link_unavailable": "🔐 Ссылка на профиль: недоступна, у тебя уже есть username",
        "profile_link_enabled": "🔐 Ссылка на профиль: включена",
        "profile_link_disabled": "🔐 Ссылка на профиль: выключена",
        "profile_start_balance": "🎯 Стартовый капитал",
        "profile_currency": "⚙️ Валюта проекта",
        "start_text": "🎮 Добро пожаловать в игрового бота.\n\nТвой стартовый баланс уже выдан: {balance}.\nНиже есть постоянное меню, как у системных ботов Telegram.",
        "top_title": "🏆 Топ игроков\n",
        "profile_link_not_available_text": "🔗 Отдельная ссылка на профиль недоступна.\n\nУ тебя уже есть username, поэтому в боте будет показываться обычное имя без отдельной ссылки.",
        "profile_link_settings_text": "🔗 Настройка ссылки на профиль\n\nПо умолчанию ссылка выключена.\nЕсли включить её, твоё имя в профиле и топе станет кликабельным.\n\nМожно управлять и командой: /profilelink on или /profilelink off",
        "profile_link_status_on": "Ссылка включена",
        "profile_link_status_off": "Ссылка выключена",
        "profile_link_status_unavailable": "Недоступно: у тебя уже есть username",
        "profile_link_updated_text": "🔗 Настройка ссылки на профиль\n\nТекущее состояние: {state}.\nЕсли ссылка включена, имя будет кликабельным в профиле и топе.",
        "profile_link_now_on": "включена",
        "profile_link_now_off": "выключена",
        "profile_link_updated_short": "Ссылка обновлена",
        "profile_link_manage_title": "🔗 Управление ссылкой на профиль\n\nСейчас: {state}.\nКоманды: /profilelink on или /profilelink off",
        "profile_link_invalid": "⚠️ Используй: /profilelink on или /profilelink off",
        "profile_link_command_updated": "🔗 Настройка обновлена.\n\nСсылка на профиль теперь {state}.",
        "profile_link_command_unavailable": "🔗 Эта настройка недоступна.\n\nУ тебя уже есть username, поэтому отдельная ссылка на профиль не используется.",
        "profile_link_button_status": "Сейчас: {status}",
        "profile_link_button_on": "🔁 Включить",
        "profile_link_button_off": "🔁 Выключить",
        "status_on": "✅ включена",
        "status_off": "❌ выключена",
        "daily_already": "🎁 Ежедневный бонус уже забран.\nСледующий бонус будет доступен через {remaining}.",
        "daily_claimed": "🎁 Бонус получен.\n\nНа счёт зачислено {reward}.\nТекущий баланс: {balance}",
        "transfer_open": "💸 Перевод шестерёнок\n\nОтправь получателя и сумму одним сообщением.\nПримеры:\n@username 500\n123456789 500",
        "transfer_cancel_button": "❌ Отменить",
        "transfer_cancelled": "❌ Перевод отменён.",
        "transfer_invalid": "⚠️ Используй формат: @username 500 или 123456789 500",
        "transfer_amount_invalid": "⚠️ Сумма перевода должна быть положительным числом.",
        "transfer_user_not_found": "❌ Получатель не найден. Он должен хотя бы раз запустить бота.",
        "transfer_self": "⚠️ Нельзя переводить шестерёнки самому себе.",
        "transfer_no_money": "❌ Недостаточно шестерёнок для перевода.",
        "transfer_done": "💸 Перевод выполнен\n\nПолучатель: {target}\nСумма: {amount}\nТвой новый баланс: {balance}",
        "trade_open": "📈 Трейд\n\nВыбери направление сделки. После этого отправь сумму ставки отдельным сообщением.",
        "trade_choose_amount": "💹 Сделка {direction}\nОтправь сумму ставки числом, например: 750",
        "trade_waiting_amount": "📈 Трейд\n\nВыбрано направление: {direction}\nТеперь отправь сумму ставки одним сообщением.",
        "trade_repeat_missing": "Сначала сыграй обычную сделку",
        "trade_repeat_button": "🔁 Повторить {bet} на {direction}",
        "bet_invalid": "⚠️ Введи сумму числом без лишних символов.",
        "bet_invalid_plain": "⚠️ Введи сумму числом без текста.",
        "trade_no_money": "❌ Недостаточно шестерёнок для такой сделки.",
        "trade_win": "✅ Сделка закрыта в плюс\n\nНаправление: {direction}\nРынок прошёл: {percent}% {market}\nПрибыль: +{profit}\nНовый баланс: {balance}",
        "trade_loss": "❌ Сделка закрыта в минус\n\nНаправление: {direction}\nРынок прошёл: {percent}% {market}\nУбыток: -{loss}\nНовый баланс: {balance}",
        "market_up": "вверх",
        "market_down": "вниз",
        "roulette_open": "🎡 Рулетка\n\nВыбери цвет, затем отправь сумму ставки.\nКоэффициенты: 🔴/⚫ x2, 🟢 x5.",
        "roulette_choose_amount": "🎡 Ставка на {color}\nОтправь сумму ставки числом.",
        "roulette_waiting_amount": "🎡 Рулетка\n\nВыбран цвет: {color}\nТеперь отправь сумму ставки одним сообщением.",
        "roulette_no_money": "❌ Недостаточно шестерёнок для ставки.",
        "roulette_repeat_missing": "Сначала сыграй обычную ставку",
        "roulette_repeat_button": "🔁 Повторить {bet} на {color}",
        "roulette_win": "🎉 Рулетка сыграла\n\nВыпало: {color}\nЧистая прибыль: +{profit}\nНовый баланс: {balance}",
        "roulette_loss": "💥 Рулетка сыграла\n\nВыпало: {color}\nПотеряно: -{bet}\nНовый баланс: {balance}",
        "mines_open": "💣 Мины\n\nНа поле 9 клеток и только 1 мина.\nНажми кнопку ниже, затем отправь сумму ставки.",
        "mines_open": "💣 Мины\n\nВыбери режим:\n1 мина -> +35% за безопасную клетку\n3 мины -> +150% за безопасную клетку\n6 мин -> +300% за безопасную клетку",
        "mines_start_1": "1 мина",
        "mines_start_3": "3 мины",
        "mines_start_6": "6 мин",
        "mines_send_bet": "💣 Отправь сумму ставки для игры в мины.",
        "mines_mode_selected": "💣 Режим выбран: {mines} мин.\nПоле: {grid}x{grid}\nНаграда за безопасную клетку: +{reward}%.\n\nТеперь отправь сумму ставки.",
        "mines_no_money": "❌ Недостаточно шестерёнок для этой ставки.",
        "mines_started": "💣 Игра началась.\nМин на поле: {mines}\nРазмер поля: {grid}x{grid}\nКаждая безопасная клетка добавляет +{reward}% к стартовой ставке.",
        "mines_not_started": "Сначала начни игру в мины",
        "mines_cell_opened": "Эта клетка уже открыта",
        "mines_boom": "💥 Мина.\n\nСтавка сгорела полностью. Попробуй ещё раз через меню.",
        "mines_safe": "✅ Клетка безопасна\n\nОткрыто клеток: {opened}\nМин на поле: {mines}\nЕсли забрать сейчас: {reward}",
        "mines_cashout_missing": "Нет активной игры",
        "mines_cashout_done": "💰 Выигрыш забран\n\nНа баланс зачислено {reward}.\nТекущий баланс: {balance}",
        "mines_cashout_button": "💰 Забрать выигрыш",
        "work_open": "🛠 Центр работ\n\nПока доступна только одна вакансия.",
        "work_loader": "📦 Грузчик",
        "work_loader_unavailable": "📦 Грузчик временно недоступен\n\nСледующая смена откроется через {remaining}.",
        "work_loader_intro": "📦 Работа: Грузчик\n\nЧтобы закончить смену, нажми кнопку «Работать» 3 раза.\nНаграда за смену: {reward}.",
        "work_loader_button": "🧱 Работать",
        "work_shift_closed": "⏳ Смена уже закрыта\n\nПопробуй снова через {remaining}.",
        "work_done": "✅ Смена завершена\n\nТы получил {reward}.\nТекущий баланс: {balance}",
        "work_progress": "📦 Работа: Грузчик\n\nПрогресс смены: {progress}/3\nНужно ещё немного поработать.",
        "fallback": "ℹ️ Используй кнопки нижнего меню.\nЕсли бот ждёт ставку, отправь только число.",
        "hours": "ч",
        "minutes": "м",
    },
    "en": {
        "coin_name": "gears",
        "menu_profile": "👤 Profile",
        "menu_top": "🏆 Top Players",
        "menu_trade": "📈 Trade",
        "menu_roulette": "🎡 Roulette",
        "menu_mines": "💣 Mines",
        "menu_work": "🛠 Work",
        "menu_daily": "🎁 Daily Bonus",
        "menu_transfer": "💸 Transfer",
        "menu_profile_link": "🔗 Profile Link",
        "menu_language": "🌐 Language",
        "menu_placeholder": "Choose a section from the menu below",
        "trade_up": "📈 up",
        "trade_down": "📉 down",
        "roulette_red": "🔴 red",
        "roulette_black": "⚫ black",
        "roulette_green": "🟢 green",
        "language_ru": "Russian",
        "language_en": "English",
        "profile_title": "🧾 Player Profile",
        "profile_name": "🪪 Name",
        "profile_balance": "💳 Balance",
        "profile_link_unavailable": "🔐 Profile link: unavailable, you already have a username",
        "profile_link_enabled": "🔐 Profile link: enabled",
        "profile_link_disabled": "🔐 Profile link: disabled",
        "profile_start_balance": "🎯 Starting Capital",
        "profile_currency": "⚙️ Project Currency",
        "start_text": "🎮 Welcome to the game bot.\n\nYour starting balance has already been credited: {balance}.\nThe persistent menu is available below, just like in Telegram system bots.",
        "top_title": "🏆 Top Players\n",
        "profile_link_not_available_text": "🔗 A separate profile link is unavailable.\n\nYou already have a username, so the bot will show your regular name without a separate link.",
        "profile_link_settings_text": "🔗 Profile link settings\n\nBy default the link is disabled.\nIf you enable it, your name in the profile and leaderboard will become clickable.\n\nYou can also manage it with: /profilelink on or /profilelink off",
        "profile_link_status_on": "Link is enabled",
        "profile_link_status_off": "Link is disabled",
        "profile_link_status_unavailable": "Unavailable: you already have a username",
        "profile_link_updated_text": "🔗 Profile link settings\n\nCurrent state: {state}.\nIf the link is enabled, your name will be clickable in the profile and leaderboard.",
        "profile_link_now_on": "enabled",
        "profile_link_now_off": "disabled",
        "profile_link_updated_short": "Link updated",
        "profile_link_manage_title": "🔗 Profile link control\n\nCurrent state: {state}.\nCommands: /profilelink on or /profilelink off",
        "profile_link_invalid": "⚠️ Use: /profilelink on or /profilelink off",
        "profile_link_command_updated": "🔗 Settings updated.\n\nProfile link is now {state}.",
        "profile_link_command_unavailable": "🔗 This setting is unavailable.\n\nYou already have a username, so a separate profile link is not used.",
        "profile_link_button_status": "Current: {status}",
        "profile_link_button_on": "🔁 Enable",
        "profile_link_button_off": "🔁 Disable",
        "status_on": "✅ enabled",
        "status_off": "❌ disabled",
        "daily_already": "🎁 Daily bonus already claimed.\nThe next bonus will be available in {remaining}.",
        "daily_claimed": "🎁 Bonus claimed.\n\n{reward} has been added to your balance.\nCurrent balance: {balance}",
        "transfer_open": "💸 Transfer gears\n\nSend the recipient and amount in one message.\nExamples:\n@username 500\n123456789 500",
        "transfer_invalid": "⚠️ Use: @username 500 or 123456789 500",
        "transfer_amount_invalid": "⚠️ Transfer amount must be a positive number.",
        "transfer_user_not_found": "❌ Recipient not found. They must start the bot at least once.",
        "transfer_self": "⚠️ You cannot transfer gears to yourself.",
        "transfer_no_money": "❌ Not enough gears for this transfer.",
        "transfer_done": "💸 Transfer completed\n\nRecipient: {target}\nAmount: {amount}\nYour new balance: {balance}",
        "trade_open": "📈 Trade\n\nChoose a direction, then send the bet amount in a separate message.",
        "trade_choose_amount": "💹 Trade {direction}\nSend the bet amount as a number, for example: 750",
        "bet_invalid": "⚠️ Enter the amount as digits without extra symbols.",
        "bet_invalid_plain": "⚠️ Enter the amount as digits without text.",
        "trade_no_money": "❌ Not enough gears for this trade.",
        "trade_win": "✅ Trade closed in profit\n\nDirection: {direction}\nMarket moved: {percent}% {market}\nProfit: +{profit}\nNew balance: {balance}",
        "trade_loss": "❌ Trade closed in loss\n\nDirection: {direction}\nMarket moved: {percent}% {market}\nLoss: -{loss}\nNew balance: {balance}",
        "market_up": "up",
        "market_down": "down",
        "roulette_open": "🎡 Roulette\n\nChoose a color, then send the bet amount.\nMultipliers: 🔴/⚫ x2, 🟢 x5.",
        "roulette_choose_amount": "🎡 Bet on {color}\nSend the bet amount as a number.",
        "roulette_no_money": "❌ Not enough gears for this bet.",
        "roulette_win": "🎉 Roulette result\n\nRolled: {color}\nNet profit: +{profit}\nNew balance: {balance}",
        "roulette_loss": "💥 Roulette result\n\nRolled: {color}\nLost: -{bet}\nNew balance: {balance}",
        "mines_open": "💣 Mines\n\nThere are 9 cells on the field and only 1 mine.\nPress the button below, then send the bet amount.",
        "mines_start_button": "💣 Start Game",
        "mines_send_bet": "💣 Send the bet amount for Mines.",
        "mines_no_money": "❌ Not enough gears for this bet.",
        "mines_started": "💣 Game started.\nOpen cells. Each safe cell adds 35% to the starting bet.",
        "mines_not_started": "Start a Mines game first",
        "mines_cell_opened": "This cell is already open",
        "mines_boom": "💥 Mine.\n\nYour bet is fully lost. Try again from the menu.",
        "mines_safe": "✅ Safe cell\n\nOpened cells: {opened}\nCash out now: {reward}",
        "mines_cashout_missing": "No active game",
        "mines_cashout_done": "💰 Winnings claimed\n\n{reward} has been credited.\nCurrent balance: {balance}",
        "mines_cashout_button": "💰 Cash Out",
        "work_open": "🛠 Job Center\n\nOnly one job is currently available.",
        "work_loader": "📦 Loader",
        "work_loader_unavailable": "📦 Loader is temporarily unavailable\n\nThe next shift opens in {remaining}.",
        "work_loader_intro": "📦 Job: Loader\n\nTo finish the shift, press the “Work” button 3 times.\nShift reward: {reward}.",
        "work_loader_button": "🧱 Work",
        "work_shift_closed": "⏳ Shift is already closed\n\nTry again in {remaining}.",
        "work_done": "✅ Shift completed\n\nYou earned {reward}.\nCurrent balance: {balance}",
        "work_progress": "📦 Job: Loader\n\nShift progress: {progress}/3\nA bit more work is needed.",
        "fallback": "ℹ️ Use the buttons in the bottom menu.\nIf the bot is waiting for a bet, send only a number.",
        "language_title": "🌐 Language Selection\n\nCurrent language: {language}.",
        "language_button": "🌐 Change Language",
        "language_updated": "🌐 Language updated: {language}.",
        "language_invalid": "⚠️ Use: /lang ru or /lang en",
        "hours": "h",
        "minutes": "m",
    },
}


class BetStates(StatesGroup):
    trade_amount = State()
    roulette_amount = State()
    mines_amount = State()
    transfer_input = State()
    admin_broadcast = State()


@dataclass
class UserProfile:
    user_id: int
    username: str | None
    full_name: str
    account_link: str
    profile_link_enabled: int
    language: str
    last_trade_bet: int | None
    last_trade_direction: str | None
    last_roulette_bet: int | None
    last_roulette_color: str | None
    last_mines_message_id: int | None
    balance: int
    last_worked_at: str | None
    work_progress: int
    last_daily_at: str | None


def load_env() -> None:
    env_path = BASE_DIR / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def now_utc() -> datetime:
    return datetime.utcnow()


def normalize_language(language_code: str | None) -> str:
    return "ru"


def tr(language: str, key: str, **kwargs: object) -> str:
    normalized = normalize_language(language)
    template = TRANSLATIONS.get(normalized, TRANSLATIONS["ru"])[key]
    if kwargs:
        return template.format(**kwargs)
    return template


def fmt_amount(value: int, language: str) -> str:
    return f"{value} {tr(language, 'coin_name')}"


def format_remaining(delta: timedelta, language: str) -> str:
    total_seconds = max(0, int(delta.total_seconds()))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    return f"{hours}{tr(language, 'hours')} {minutes}{tr(language, 'minutes')}"


def display_name(profile: UserProfile) -> str:
    if profile.username:
        return f"@{profile.username}"
    return profile.full_name


def display_name_html(profile: UserProfile) -> str:
    label = html.escape(display_name(profile))
    if not profile.username and profile.profile_link_enabled and profile.account_link:
        return f'<a href="{html.escape(profile.account_link, quote=True)}">{label}</a>'
    return label


def build_account_link(user_id: int, username: str | None) -> str:
    if username:
        return f"https://t.me/{username}"
    return f"tg://user?id={user_id}"


def trade_label(language: str, direction: str) -> str:
    return tr(language, f"trade_{direction}")


def roulette_label(language: str, color: str) -> str:
    return tr(language, f"roulette_{color}")


def mines_grid_size(mine_count: int) -> int:
    return 4 if mine_count >= 6 else 3


def mines_reward_percent(mine_count: int) -> int:
    return MINES_REWARD_PERCENTS.get(mine_count, 35)


class Storage:
    def __init__(self, path: Path) -> None:
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT NOT NULL DEFAULT 'Игрок',
                account_link TEXT NOT NULL DEFAULT '',
                profile_link_enabled INTEGER NOT NULL DEFAULT 0,
                language TEXT NOT NULL DEFAULT 'ru',
                last_trade_bet INTEGER,
                last_trade_direction TEXT,
                last_roulette_bet INTEGER,
                last_roulette_color TEXT,
                last_mines_message_id INTEGER,
                balance INTEGER NOT NULL,
                last_worked_at TEXT,
                work_progress INTEGER NOT NULL DEFAULT 0,
                last_daily_at TEXT
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mines_games (
                user_id INTEGER PRIMARY KEY,
                bet INTEGER NOT NULL,
                safe_cells TEXT NOT NULL,
                opened_cells TEXT NOT NULL,
                mine_cell INTEGER NOT NULL,
                mine_cells TEXT NOT NULL DEFAULT '',
                grid_size INTEGER NOT NULL DEFAULT 3,
                mine_count INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        self._ensure_user_columns()
        self._ensure_mines_columns()
        self._backfill_mines_data()
        self._backfill_account_links()
        self.conn.commit()

    def _ensure_user_columns(self) -> None:
        existing_columns = {
            row["name"] for row in self.conn.execute("PRAGMA table_info(users)").fetchall()
        }
        migrations = {
            "username": "ALTER TABLE users ADD COLUMN username TEXT",
            "full_name": "ALTER TABLE users ADD COLUMN full_name TEXT NOT NULL DEFAULT 'Игрок'",
            "account_link": "ALTER TABLE users ADD COLUMN account_link TEXT NOT NULL DEFAULT ''",
            "profile_link_enabled": "ALTER TABLE users ADD COLUMN profile_link_enabled INTEGER NOT NULL DEFAULT 0",
            "language": "ALTER TABLE users ADD COLUMN language TEXT NOT NULL DEFAULT 'ru'",
            "last_trade_bet": "ALTER TABLE users ADD COLUMN last_trade_bet INTEGER",
            "last_trade_direction": "ALTER TABLE users ADD COLUMN last_trade_direction TEXT",
            "last_roulette_bet": "ALTER TABLE users ADD COLUMN last_roulette_bet INTEGER",
            "last_roulette_color": "ALTER TABLE users ADD COLUMN last_roulette_color TEXT",
            "last_mines_message_id": "ALTER TABLE users ADD COLUMN last_mines_message_id INTEGER",
            "last_daily_at": "ALTER TABLE users ADD COLUMN last_daily_at TEXT",
        }
        for column, query in migrations.items():
            if column not in existing_columns:
                self.conn.execute(query)

    def _backfill_account_links(self) -> None:
        rows = self.conn.execute("SELECT user_id, username, account_link FROM users").fetchall()
        for row in rows:
            account_link = build_account_link(row["user_id"], row["username"])
            if row["account_link"] != account_link:
                self.conn.execute(
                    "UPDATE users SET account_link = ? WHERE user_id = ?",
                    (account_link, row["user_id"]),
                )

    def _ensure_mines_columns(self) -> None:
        existing_columns = {
            row["name"] for row in self.conn.execute("PRAGMA table_info(mines_games)").fetchall()
        }
        migrations = {
            "mine_cells": "ALTER TABLE mines_games ADD COLUMN mine_cells TEXT NOT NULL DEFAULT ''",
            "grid_size": "ALTER TABLE mines_games ADD COLUMN grid_size INTEGER NOT NULL DEFAULT 3",
            "mine_count": "ALTER TABLE mines_games ADD COLUMN mine_count INTEGER NOT NULL DEFAULT 1",
        }
        for column, query in migrations.items():
            if column not in existing_columns:
                self.conn.execute(query)

    def _backfill_mines_data(self) -> None:
        rows = self.conn.execute(
            "SELECT user_id, mine_cell, mine_cells, grid_size, mine_count FROM mines_games"
        ).fetchall()
        for row in rows:
            mine_cells = row["mine_cells"] or str(row["mine_cell"])
            grid_size = row["grid_size"] or 3
            mine_count = row["mine_count"] or len([x for x in mine_cells.split(",") if x]) or 1
            if (
                row["mine_cells"] != mine_cells
                or row["grid_size"] != grid_size
                or row["mine_count"] != mine_count
            ):
                self.conn.execute(
                    "UPDATE mines_games SET mine_cells = ?, grid_size = ?, mine_count = ? WHERE user_id = ?",
                    (mine_cells, grid_size, mine_count, row["user_id"]),
                )

    def get_or_create_user(
        self,
        user_id: int,
        username: str | None = None,
        full_name: str | None = None,
        language: str | None = None,
    ) -> UserProfile:
        resolved_language = normalize_language(language)
        row = self.conn.execute(
            """
            SELECT
                user_id, username, full_name, account_link, profile_link_enabled, language,
                last_trade_bet, last_trade_direction,
                last_roulette_bet, last_roulette_color,
                last_mines_message_id,
                balance, last_worked_at, work_progress, last_daily_at
            FROM users
            WHERE user_id = ?
            """,
            (user_id,),
        ).fetchone()

        if row is None:
            resolved_name = full_name or "Игрок"
            account_link = build_account_link(user_id, username)
            self.conn.execute(
                """
                INSERT INTO users(
                    user_id, username, full_name, account_link, profile_link_enabled, language,
                    last_trade_bet, last_trade_direction,
                    last_roulette_bet, last_roulette_color,
                    last_mines_message_id,
                    balance, last_worked_at, work_progress, last_daily_at
                )
                VALUES(?, ?, ?, ?, 0, ?, NULL, NULL, NULL, NULL, NULL, ?, NULL, 0, NULL)
                """,
                (user_id, username, resolved_name, account_link, resolved_language, START_BALANCE),
            )
            self.conn.commit()
            return UserProfile(
                user_id=user_id,
                username=username,
                full_name=resolved_name,
                account_link=account_link,
                profile_link_enabled=0,
                language=resolved_language,
                last_trade_bet=None,
                last_trade_direction=None,
                last_roulette_bet=None,
                last_roulette_color=None,
                last_mines_message_id=None,
                balance=START_BALANCE,
                last_worked_at=None,
                work_progress=0,
                last_daily_at=None,
            )

        profile = UserProfile(**dict(row))
        next_username = username if username is not None else profile.username
        next_full_name = full_name or profile.full_name
        next_account_link = build_account_link(user_id, next_username)
        if (
            next_username != profile.username
            or next_full_name != profile.full_name
            or profile.account_link != next_account_link
        ):
            self.conn.execute(
                "UPDATE users SET username = ?, full_name = ?, account_link = ? WHERE user_id = ?",
                (next_username, next_full_name, next_account_link, user_id),
            )
            self.conn.commit()
            profile.username = next_username
            profile.full_name = next_full_name
            profile.account_link = next_account_link
        return profile

    def update_balance(self, user_id: int, delta: int) -> int:
        self.conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (delta, user_id))
        self.conn.commit()
        return self.get_or_create_user(user_id).balance

    def save_last_roulette_bet(self, user_id: int, bet: int, color: str) -> None:
        self.conn.execute(
            "UPDATE users SET last_roulette_bet = ?, last_roulette_color = ? WHERE user_id = ?",
            (bet, color, user_id),
        )
        self.conn.commit()

    def set_last_mines_message_id(self, user_id: int, message_id: int | None) -> None:
        self.conn.execute(
            "UPDATE users SET last_mines_message_id = ? WHERE user_id = ?",
            (message_id, user_id),
        )
        self.conn.commit()

    def save_last_trade_bet(self, user_id: int, bet: int, direction: str) -> None:
        self.conn.execute(
            "UPDATE users SET last_trade_bet = ?, last_trade_direction = ? WHERE user_id = ?",
            (bet, direction, user_id),
        )
        self.conn.commit()

    def set_profile_link_enabled(self, user_id: int, enabled: bool) -> None:
        self.conn.execute(
            "UPDATE users SET profile_link_enabled = ? WHERE user_id = ?",
            (1 if enabled else 0, user_id),
        )
        self.conn.commit()

    def set_work_progress(self, user_id: int, progress: int) -> None:
        self.conn.execute("UPDATE users SET work_progress = ? WHERE user_id = ?", (progress, user_id))
        self.conn.commit()

    def complete_work(self, user_id: int) -> int:
        self.conn.execute(
            "UPDATE users SET balance = balance + ?, last_worked_at = ?, work_progress = 0 WHERE user_id = ?",
            (WORK_REWARD, now_utc().isoformat(), user_id),
        )
        self.conn.commit()
        return self.get_or_create_user(user_id).balance

    def can_work(self, user_id: int) -> tuple[bool, timedelta | None]:
        user = self.get_or_create_user(user_id)
        if not user.last_worked_at:
            return True, None
        last_work = datetime.fromisoformat(user.last_worked_at)
        available_at = last_work + timedelta(hours=WORK_COOLDOWN_HOURS)
        remaining = available_at - now_utc()
        if remaining.total_seconds() <= 0:
            return True, None
        return False, remaining

    def can_claim_daily(self, user_id: int) -> tuple[bool, timedelta | None]:
        user = self.get_or_create_user(user_id)
        if not user.last_daily_at:
            return True, None
        last_daily = datetime.fromisoformat(user.last_daily_at)
        available_at = last_daily + timedelta(hours=DAILY_COOLDOWN_HOURS)
        remaining = available_at - now_utc()
        if remaining.total_seconds() <= 0:
            return True, None
        return False, remaining

    def claim_daily(self, user_id: int) -> int:
        self.conn.execute(
            "UPDATE users SET balance = balance + ?, last_daily_at = ? WHERE user_id = ?",
            (DAILY_REWARD, now_utc().isoformat(), user_id),
        )
        self.conn.commit()
        return self.get_or_create_user(user_id).balance

    def top_users(self, limit: int = 10) -> list[UserProfile]:
        rows = self.conn.execute(
            """
            SELECT
                user_id, username, full_name, account_link, profile_link_enabled, language,
                last_trade_bet, last_trade_direction,
                last_roulette_bet, last_roulette_color,
                last_mines_message_id,
                balance, last_worked_at, work_progress, last_daily_at
            FROM users
            ORDER BY balance DESC, user_id ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [UserProfile(**dict(row)) for row in rows]

    def find_user(self, identifier: str) -> UserProfile | None:
        cleaned = identifier.strip()
        if cleaned.startswith("@"):
            row = self.conn.execute(
                """
                SELECT
                    user_id, username, full_name, account_link, profile_link_enabled, language,
                    last_trade_bet, last_trade_direction,
                    last_roulette_bet, last_roulette_color,
                    last_mines_message_id,
                    balance, last_worked_at, work_progress, last_daily_at
                FROM users
                WHERE lower(username) = lower(?)
                """,
                (cleaned[1:],),
            ).fetchone()
        elif cleaned.isdigit():
            row = self.conn.execute(
                """
                SELECT
                    user_id, username, full_name, account_link, profile_link_enabled, language,
                    last_trade_bet, last_trade_direction,
                    last_roulette_bet, last_roulette_color,
                    last_mines_message_id,
                    balance, last_worked_at, work_progress, last_daily_at
                FROM users
                WHERE user_id = ?
                """,
                (int(cleaned),),
            ).fetchone()
        else:
            return None

        if row is None:
            return None
        return UserProfile(**dict(row))

    def transfer_balance(self, sender_id: int, recipient_id: int, amount: int) -> int:
        self.conn.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (amount, sender_id))
        self.conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, recipient_id))
        self.conn.commit()
        return self.get_or_create_user(sender_id).balance

    def save_mines_game(
        self,
        user_id: int,
        bet: int,
        safe_cells: list[int],
        opened_cells: list[int],
        mine_cells: list[int],
        grid_size: int,
        mine_count: int,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO mines_games(user_id, bet, safe_cells, opened_cells, mine_cell, mine_cells, grid_size, mine_count)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                bet = excluded.bet,
                safe_cells = excluded.safe_cells,
                opened_cells = excluded.opened_cells,
                mine_cell = excluded.mine_cell,
                mine_cells = excluded.mine_cells,
                grid_size = excluded.grid_size,
                mine_count = excluded.mine_count
            """,
            (
                user_id,
                bet,
                ",".join(map(str, safe_cells)),
                ",".join(map(str, opened_cells)),
                mine_cells[0],
                ",".join(map(str, mine_cells)),
                grid_size,
                mine_count,
            ),
        )
        self.conn.commit()

    def get_mines_game(self, user_id: int) -> dict | None:
        row = self.conn.execute(
            "SELECT bet, safe_cells, opened_cells, mine_cell, mine_cells, grid_size, mine_count FROM mines_games WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        if row is None:
            return None
        mine_cells = [int(x) for x in (row["mine_cells"] or "").split(",") if x]
        if not mine_cells:
            mine_cells = [row["mine_cell"]]
        return {
            "bet": row["bet"],
            "safe_cells": [int(x) for x in row["safe_cells"].split(",") if x],
            "opened_cells": [int(x) for x in row["opened_cells"].split(",") if x],
            "mine_cells": mine_cells,
            "grid_size": row["grid_size"],
            "mine_count": row["mine_count"],
        }

    def delete_mines_game(self, user_id: int) -> None:
        self.conn.execute("DELETE FROM mines_games WHERE user_id = ?", (user_id,))
        self.conn.commit()


storage = Storage(DB_PATH)
dp = Dispatcher()


def main_menu(language: str) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=tr(language, "menu_profile")), KeyboardButton(text=tr(language, "menu_top"))],
            [KeyboardButton(text=tr(language, "menu_trade")), KeyboardButton(text=tr(language, "menu_roulette"))],
            [KeyboardButton(text=tr(language, "menu_mines")), KeyboardButton(text=tr(language, "menu_work"))],
            [KeyboardButton(text=tr(language, "menu_daily")), KeyboardButton(text=tr(language, "menu_transfer"))],
            [KeyboardButton(text=tr(language, "menu_profile_link"))],
        ],
        resize_keyboard=True,
        input_field_placeholder=tr(language, "menu_placeholder"),
    )


def trade_menu(language: str, last_bet: int | None = None, last_direction: str | None = None) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=trade_label(language, "up"), callback_data="trade:up")],
        [InlineKeyboardButton(text=trade_label(language, "down"), callback_data="trade:down")],
    ]
    if last_bet and last_direction:
        rows.append(
            [
                InlineKeyboardButton(
                    text=tr(
                        language,
                        "trade_repeat_button",
                        bet=last_bet,
                        direction=trade_label(language, last_direction),
                    ),
                    callback_data="trade:repeat",
                )
            ]
        )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def roulette_menu(language: str, last_bet: int | None = None, last_color: str | None = None) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=roulette_label(language, "red"), callback_data="roulette:red")],
        [InlineKeyboardButton(text=roulette_label(language, "black"), callback_data="roulette:black")],
        [InlineKeyboardButton(text=roulette_label(language, "green"), callback_data="roulette:green")],
    ]
    if last_bet and last_color:
        rows.append(
            [
                InlineKeyboardButton(
                    text=tr(
                        language,
                        "roulette_repeat_button",
                        bet=last_bet,
                        color=roulette_label(language, last_color),
                    ),
                    callback_data="roulette:repeat",
                )
            ]
        )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def mines_start_menu(language: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=tr(language, "mines_start_1"), callback_data="mines:mode:1"),
                InlineKeyboardButton(text=tr(language, "mines_start_3"), callback_data="mines:mode:3"),
                InlineKeyboardButton(text=tr(language, "mines_start_6"), callback_data="mines:mode:6"),
            ],
        ]
    )


def mines_grid(game: dict, language: str) -> InlineKeyboardMarkup:
    rows = []
    grid_size = game["grid_size"]
    for row in range(grid_size):
        buttons = []
        for col in range(grid_size):
            cell = row * grid_size + col
            label = "💎" if cell in game["opened_cells"] else "❔"
            buttons.append(InlineKeyboardButton(text=label, callback_data=f"mines:open:{cell}"))
        rows.append(buttons)
    rows.append([InlineKeyboardButton(text=tr(language, "mines_cashout_button"), callback_data="mines:cashout")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def work_menu(language: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=tr(language, "work_loader"), callback_data="work:loader")],
        ]
    )


def loader_button(language: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=tr(language, "work_loader_button"), callback_data="work:loader:do")],
        ]
    )


def profile_link_menu(language: str, enabled: bool) -> InlineKeyboardMarkup:
    status_text = tr(language, "status_on") if enabled else tr(language, "status_off")
    next_action = "off" if enabled else "on"
    next_label = tr(language, "profile_link_button_off") if enabled else tr(language, "profile_link_button_on")
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=tr(language, "profile_link_button_status", status=status_text),
                    callback_data="profilelink:status",
                )
            ],
            [
                InlineKeyboardButton(
                    text=next_label,
                    callback_data=f"profilelink:{next_action}",
                )
            ],
        ]
    )


def transfer_menu(language: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=tr(language, "transfer_cancel_button"), callback_data="transfer:cancel")]
        ]
    )


def admin_menu(language: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=tr(language, "admin_broadcast_button"), callback_data="admin:broadcast")]
        ]
    )


def admin_broadcast_menu(language: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=tr(language, "admin_broadcast_cancel"), callback_data="admin:broadcast:cancel")]
        ]
    )


def ensure_registered(message: Message) -> UserProfile:
    return storage.get_or_create_user(
        user_id=message.from_user.id,
        username=message.from_user.username,
        full_name=message.from_user.full_name,
        language="ru",
    )


def is_admin(user_id: int) -> bool:
    return ADMIN_ID > 0 and user_id == ADMIN_ID


def ensure_balance(user_id: int, amount: int) -> bool:
    return storage.get_or_create_user(user_id).balance >= amount


def parse_bet(value: str) -> int | None:
    cleaned = value.replace(" ", "")
    if not cleaned.isdigit():
        return None
    bet = int(cleaned)
    if bet <= 0:
        return None
    return bet


def parse_transfer_input(value: str) -> tuple[str, int] | None:
    parts = value.split()
    if len(parts) != 2:
        return None
    recipient, amount_raw = parts
    if recipient.startswith("@"):
        if len(recipient) < 2:
            return None
    elif not recipient.isdigit():
        return None
    amount = parse_bet(amount_raw)
    if amount is None:
        return None
    return recipient, amount


def profile_text(user: UserProfile) -> str:
    profile_link_line = ""
    if not user.username:
        profile_link_line = (
            tr(
                user.language,
                "profile_link_enabled" if user.profile_link_enabled else "profile_link_disabled",
            )
            + "\n"
        )
    return (
        f"{tr(user.language, 'profile_title')}\n\n"
        f"{tr(user.language, 'profile_name')}: {display_name_html(user)}\n"
        f"{tr(user.language, 'profile_balance')}: {fmt_amount(user.balance, user.language)}\n"
        f"{profile_link_line}"
        f"{tr(user.language, 'profile_start_balance')}: {fmt_amount(START_BALANCE, user.language)}\n"
        f"{tr(user.language, 'profile_currency')}: {tr(user.language, 'coin_name')}"
    )


async def send_main_menu(message: Message, text: str, language: str) -> None:
    await message.answer(text, reply_markup=main_menu(language), parse_mode="HTML")


async def edit_message_text_safe(
    bot: Bot,
    chat_id: int,
    message_id: int,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    try:
        await bot.edit_message_text(
            text=text,
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=reply_markup,
        )
    except TelegramBadRequest as exc:
        if "message is not modified" not in str(exc).lower():
            raise


def transfer_prompt_text(user: UserProfile, error_text: str | None = None) -> str:
    if not error_text:
        return tr(user.language, "transfer_open")
    return (
        f"<blockquote><b>{html.escape(error_text)}</b></blockquote>\n\n"
        f"{tr(user.language, 'transfer_open')}"
    )


async def upsert_mines_panel(
    message: Message,
    user: UserProfile,
    text: str,
    reply_markup: InlineKeyboardMarkup,
) -> tuple[int, int]:
    if user.last_mines_message_id:
        try:
            await message.bot.edit_message_text(
                text=text,
                chat_id=message.chat.id,
                message_id=user.last_mines_message_id,
                reply_markup=reply_markup,
            )
            return message.chat.id, user.last_mines_message_id
        except TelegramBadRequest:
            storage.set_last_mines_message_id(user.user_id, None)

    panel = await message.answer(text, reply_markup=reply_markup)
    storage.set_last_mines_message_id(user.user_id, panel.message_id)
    return panel.chat.id, panel.message_id


def build_roulette_result_text(user: UserProfile, color: str, bet: int) -> str:
    spin = random.choices(ROULETTE_COLORS, weights=[48, 48, 4], k=1)[0]
    storage.save_last_roulette_bet(user.user_id, bet, color)

    if spin == color:
        multiplier = 5 if color == "green" else 2
        profit = bet * (multiplier - 1)
        balance = storage.update_balance(user.user_id, profit)
        return tr(
            user.language,
            "roulette_win",
            color=roulette_label(user.language, spin),
            profit=fmt_amount(profit, user.language),
            balance=fmt_amount(balance, user.language),
        )

    balance = storage.update_balance(user.user_id, -bet)
    return tr(
        user.language,
        "roulette_loss",
        color=roulette_label(user.language, spin),
        bet=fmt_amount(bet, user.language),
        balance=fmt_amount(balance, user.language),
    )


def build_trade_result_text(user: UserProfile, direction: str, bet: int) -> str:
    played_up = random.choice([True, False])
    market_direction = "up" if played_up else "down"
    percent = random.randint(5, 32)
    storage.save_last_trade_bet(user.user_id, bet, direction)

    if direction == market_direction:
        profit = max(1, bet * percent // 100)
        balance = storage.update_balance(user.user_id, profit)
        return tr(
            user.language,
            "trade_win",
            direction=trade_label(user.language, direction),
            percent=percent,
            market=tr(user.language, "market_up" if played_up else "market_down"),
            profit=fmt_amount(profit, user.language),
            balance=fmt_amount(balance, user.language),
        )

    loss = max(1, bet * percent // 100)
    loss = min(loss, bet)
    balance = storage.update_balance(user.user_id, -loss)
    return tr(
        user.language,
        "trade_loss",
        direction=trade_label(user.language, direction),
        percent=percent,
        market=tr(user.language, "market_up" if played_up else "market_down"),
        loss=fmt_amount(loss, user.language),
        balance=fmt_amount(balance, user.language),
    )


@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    await state.clear()
    user = ensure_registered(message)
    await send_main_menu(
        message,
        tr(user.language, "start_text", balance=fmt_amount(user.balance, user.language)),
        user.language,
    )


@dp.message(Command("myid"))
async def my_id(message: Message, state: FSMContext) -> None:
    await state.clear()
    user = ensure_registered(message)
    await send_main_menu(
        message,
        tr(user.language, "my_id", user_id=message.from_user.id),
        user.language,
    )


@dp.message(Command("admin"))
async def admin_panel(message: Message, state: FSMContext) -> None:
    await state.clear()
    user = ensure_registered(message)
    if not is_admin(message.from_user.id):
        await send_main_menu(message, tr(user.language, "admin_only"), user.language)
        return
    await message.answer(
        tr(user.language, "admin_panel"),
        reply_markup=admin_menu(user.language),
    )


@dp.callback_query(F.data == "admin:broadcast")
async def admin_broadcast_start(callback: CallbackQuery, state: FSMContext) -> None:
    user = storage.get_or_create_user(
        callback.from_user.id,
        callback.from_user.username,
        callback.from_user.full_name,
        "ru",
    )
    if not is_admin(callback.from_user.id):
        await callback.answer(tr(user.language, "admin_only"), show_alert=True)
        return
    await state.set_state(BetStates.admin_broadcast)
    await callback.message.edit_text(
        tr(user.language, "admin_broadcast_prompt"),
        reply_markup=admin_broadcast_menu(user.language),
    )
    await callback.answer()


@dp.callback_query(F.data == "admin:broadcast:cancel")
async def admin_broadcast_cancel(callback: CallbackQuery, state: FSMContext) -> None:
    user = storage.get_or_create_user(
        callback.from_user.id,
        callback.from_user.username,
        callback.from_user.full_name,
        "ru",
    )
    await state.clear()
    await callback.message.edit_text(
        tr(user.language, "admin_broadcast_cancelled"),
        reply_markup=admin_menu(user.language),
    )
    await callback.answer()


@dp.message(BetStates.admin_broadcast)
async def admin_broadcast_send(message: Message, state: FSMContext, bot: Bot) -> None:
    user = ensure_registered(message)
    if not is_admin(message.from_user.id):
        await state.clear()
        await send_main_menu(message, tr(user.language, "admin_only"), user.language)
        return

    text = (message.text or "").strip()
    if not text:
        await message.answer(tr(user.language, "admin_broadcast_prompt"), reply_markup=admin_broadcast_menu(user.language))
        return

    status_message = await message.answer(tr(user.language, "admin_broadcast_running"))
    sent = 0
    failed = 0
    rows = storage.conn.execute("SELECT user_id FROM users ORDER BY user_id ASC").fetchall()
    for row in rows:
        target_user_id = row["user_id"]
        try:
            await bot.send_message(chat_id=target_user_id, text=text)
            sent += 1
        except Exception:
            failed += 1

    await state.clear()
    await status_message.edit_text(
        tr(user.language, "admin_broadcast_done", sent=sent, failed=failed),
        reply_markup=admin_menu(user.language),
    )
    try:
        await message.delete()
    except TelegramBadRequest:
        pass


@dp.message(F.text == "👤 Профиль")
@dp.message(F.text == "👤 Profile")
async def show_profile(message: Message, state: FSMContext) -> None:
    await state.clear()
    user = ensure_registered(message)
    await send_main_menu(message, profile_text(user), user.language)


@dp.message(F.text == "🏆 Топ игроков")
@dp.message(F.text == "🏆 Top Players")
async def show_top(message: Message, state: FSMContext) -> None:
    await state.clear()
    requester = ensure_registered(message)
    leaders = storage.top_users()
    lines = [tr(requester.language, "top_title")]
    medals = ["🥇", "🥈", "🥉"]
    for index, user in enumerate(leaders, start=1):
        prefix = medals[index - 1] if index <= 3 else f"{index}."
        lines.append(f"{prefix} {display_name_html(user)} — {fmt_amount(user.balance, requester.language)}")
    await send_main_menu(message, "\n".join(lines), requester.language)


@dp.message(F.text == "🔗 Ссылка на профиль")
@dp.message(F.text == "🔗 Profile Link")
async def open_profile_link_settings(message: Message, state: FSMContext) -> None:
    await state.clear()
    user = ensure_registered(message)
    if user.username:
        await send_main_menu(
            message,
            tr(user.language, "profile_link_not_available_text"),
            user.language,
        )
        return
    await message.answer(
        tr(user.language, "profile_link_settings_text"),
        reply_markup=profile_link_menu(user.language, bool(user.profile_link_enabled)),
    )


@dp.callback_query(F.data == "profilelink:status")
async def profile_link_status(callback: CallbackQuery) -> None:
    user = storage.get_or_create_user(
        callback.from_user.id,
        callback.from_user.username,
        callback.from_user.full_name,
        "ru",
    )
    if user.username:
        await callback.answer(tr(user.language, "profile_link_status_unavailable"), show_alert=True)
        return
    await callback.answer(
        tr(user.language, "profile_link_status_on" if user.profile_link_enabled else "profile_link_status_off"),
        show_alert=True,
    )


@dp.callback_query(F.data.startswith("profilelink:"))
async def toggle_profile_link(callback: CallbackQuery) -> None:
    action = callback.data.split(":")[1]
    if action == "status":
        return

    user = storage.get_or_create_user(
        callback.from_user.id,
        callback.from_user.username,
        callback.from_user.full_name,
        "ru",
    )
    if user.username:
        await callback.answer(tr(user.language, "profile_link_status_unavailable"), show_alert=True)
        return
    enabled = action == "on"
    storage.set_profile_link_enabled(user.user_id, enabled)
    updated_user = storage.get_or_create_user(
        callback.from_user.id,
        callback.from_user.username,
        callback.from_user.full_name,
        "ru",
    )
    await callback.message.edit_text(
        tr(
            updated_user.language,
            "profile_link_updated_text",
            state=tr(updated_user.language, "profile_link_now_on" if enabled else "profile_link_now_off"),
        ),
        reply_markup=profile_link_menu(updated_user.language, bool(updated_user.profile_link_enabled)),
    )
    await callback.answer(tr(updated_user.language, "profile_link_updated_short"))


@dp.message(Command("profilelink"))
async def profile_link_command(message: Message, state: FSMContext) -> None:
    await state.clear()
    user = ensure_registered(message)
    if user.username:
        await send_main_menu(
            message,
            tr(user.language, "profile_link_command_unavailable"),
            user.language,
        )
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) == 1:
        await send_main_menu(
            message,
            tr(
                user.language,
                "profile_link_manage_title",
                state=tr(user.language, "profile_link_now_on" if user.profile_link_enabled else "profile_link_now_off"),
            ),
            user.language,
        )
        return

    action = parts[1].strip().lower()
    if action not in {"on", "off"}:
        await send_main_menu(
            message,
            tr(user.language, "profile_link_invalid"),
            user.language,
        )
        return

    storage.set_profile_link_enabled(user.user_id, action == "on")
    updated_user = storage.get_or_create_user(
        message.from_user.id,
        message.from_user.username,
        message.from_user.full_name,
        "ru",
    )
    await send_main_menu(
        message,
        tr(
            updated_user.language,
            "profile_link_command_updated",
            state=tr(
                updated_user.language,
                "profile_link_now_on" if updated_user.profile_link_enabled else "profile_link_now_off",
            ),
        ),
        updated_user.language,
    )


@dp.message(F.text == "🎁 Ежедневный бонус")
@dp.message(F.text == "🎁 Daily Bonus")
async def claim_daily_bonus(message: Message, state: FSMContext) -> None:
    await state.clear()
    user = ensure_registered(message)
    can_claim, remaining = storage.can_claim_daily(user.user_id)
    if not can_claim:
        await send_main_menu(
            message,
            tr(user.language, "daily_already", remaining=format_remaining(remaining, user.language)),
            user.language,
        )
        return

    balance = storage.claim_daily(user.user_id)
    await send_main_menu(
        message,
        tr(
            user.language,
            "daily_claimed",
            reward=fmt_amount(DAILY_REWARD, user.language),
            balance=fmt_amount(balance, user.language),
        ),
        user.language,
    )


@dp.message(F.text == "💸 Перевод")
@dp.message(F.text == "💸 Transfer")
async def open_transfer(message: Message, state: FSMContext) -> None:
    await state.clear()
    user = ensure_registered(message)
    await state.set_state(BetStates.transfer_input)
    prompt = await message.answer(
        transfer_prompt_text(user),
        reply_markup=transfer_menu(user.language),
        parse_mode="HTML",
    )
    await state.update_data(
        transfer_chat_id=prompt.chat.id,
        transfer_message_id=prompt.message_id,
    )


@dp.callback_query(F.data == "transfer:cancel")
async def cancel_transfer(callback: CallbackQuery, state: FSMContext) -> None:
    user = storage.get_or_create_user(
        callback.from_user.id,
        callback.from_user.username,
        callback.from_user.full_name,
        "ru",
    )
    await state.clear()
    await callback.message.edit_text(tr(user.language, "transfer_cancelled"))
    await callback.answer()


@dp.message(BetStates.transfer_input)
async def process_transfer(message: Message, state: FSMContext) -> None:
    user = ensure_registered(message)
    data = await state.get_data()
    transfer_chat_id = data.get("transfer_chat_id")
    transfer_message_id = data.get("transfer_message_id")
    parsed = parse_transfer_input(message.text or "")
    if parsed is None:
        if transfer_chat_id and transfer_message_id:
            await edit_message_text_safe(
                message.bot,
                transfer_chat_id,
                transfer_message_id,
                transfer_prompt_text(user, tr(user.language, "transfer_invalid")),
                reply_markup=transfer_menu(user.language),
            )
            try:
                await message.delete()
            except TelegramBadRequest:
                pass
        else:
            await message.answer(tr(user.language, "transfer_invalid"))
        return

    recipient_raw, amount = parsed
    if amount <= 0:
        if transfer_chat_id and transfer_message_id:
            await edit_message_text_safe(
                message.bot,
                transfer_chat_id,
                transfer_message_id,
                transfer_prompt_text(user, tr(user.language, "transfer_amount_invalid")),
                reply_markup=transfer_menu(user.language),
            )
            try:
                await message.delete()
            except TelegramBadRequest:
                pass
        else:
            await message.answer(tr(user.language, "transfer_amount_invalid"))
        return

    recipient = storage.find_user(recipient_raw)
    if recipient is None:
        if transfer_chat_id and transfer_message_id:
            await edit_message_text_safe(
                message.bot,
                transfer_chat_id,
                transfer_message_id,
                transfer_prompt_text(user, tr(user.language, "transfer_user_not_found")),
                reply_markup=transfer_menu(user.language),
            )
            try:
                await message.delete()
            except TelegramBadRequest:
                pass
        else:
            await message.answer(tr(user.language, "transfer_user_not_found"))
        return

    if recipient.user_id == user.user_id:
        if transfer_chat_id and transfer_message_id:
            await edit_message_text_safe(
                message.bot,
                transfer_chat_id,
                transfer_message_id,
                transfer_prompt_text(user, tr(user.language, "transfer_self")),
                reply_markup=transfer_menu(user.language),
            )
            try:
                await message.delete()
            except TelegramBadRequest:
                pass
        else:
            await message.answer(tr(user.language, "transfer_self"))
        return

    if not ensure_balance(user.user_id, amount):
        if transfer_chat_id and transfer_message_id:
            await edit_message_text_safe(
                message.bot,
                transfer_chat_id,
                transfer_message_id,
                transfer_prompt_text(user, tr(user.language, "transfer_no_money")),
                reply_markup=transfer_menu(user.language),
            )
            try:
                await message.delete()
            except TelegramBadRequest:
                pass
        else:
            await send_main_menu(message, tr(user.language, "transfer_no_money"), user.language)
        return

    balance = storage.transfer_balance(user.user_id, recipient.user_id, amount)
    await state.clear()
    result_text = tr(
        user.language,
        "transfer_done",
        target=display_name(recipient),
        amount=fmt_amount(amount, user.language),
        balance=fmt_amount(balance, user.language),
    )
    if transfer_chat_id and transfer_message_id:
        await edit_message_text_safe(
            message.bot,
            transfer_chat_id,
            transfer_message_id,
            result_text,
        )
        try:
            await message.delete()
        except TelegramBadRequest:
            pass
    else:
        await send_main_menu(message, result_text, user.language)

@dp.message(F.text == "📈 Трейд")
@dp.message(F.text == "📈 Trade")
async def open_trade(message: Message, state: FSMContext) -> None:
    await state.clear()
    user = ensure_registered(message)
    await message.answer(
        tr(user.language, "trade_open"),
        reply_markup=trade_menu(user.language, user.last_trade_bet, user.last_trade_direction),
    )


@dp.callback_query(F.data.startswith("trade:"))
async def choose_trade(callback: CallbackQuery, state: FSMContext) -> None:
    user = storage.get_or_create_user(
        callback.from_user.id,
        callback.from_user.username,
        callback.from_user.full_name,
        "ru",
    )
    action = callback.data.split(":")[1]
    if action == "repeat":
        if not user.last_trade_bet or not user.last_trade_direction:
            await callback.answer(tr(user.language, "trade_repeat_missing"), show_alert=True)
            return
        if not ensure_balance(user.user_id, user.last_trade_bet):
            await callback.answer(tr(user.language, "trade_no_money"), show_alert=True)
            return
        text = build_trade_result_text(user, user.last_trade_direction, user.last_trade_bet)
        updated_user = storage.get_or_create_user(
            callback.from_user.id,
            callback.from_user.username,
            callback.from_user.full_name,
            "ru",
        )
        await callback.message.edit_text(
            text,
            reply_markup=trade_menu(
                updated_user.language,
                updated_user.last_trade_bet,
                updated_user.last_trade_direction,
            ),
        )
        await state.clear()
        await callback.answer()
        return

    direction = action
    await state.set_state(BetStates.trade_amount)
    await state.update_data(
        direction=direction,
        trade_chat_id=callback.message.chat.id,
        trade_message_id=callback.message.message_id,
    )
    await callback.message.edit_text(
        tr(user.language, "trade_waiting_amount", direction=trade_label(user.language, direction)),
        reply_markup=trade_menu(user.language, user.last_trade_bet, user.last_trade_direction),
    )
    await callback.answer()


@dp.message(BetStates.trade_amount)
async def process_trade_bet(message: Message, state: FSMContext) -> None:
    user = ensure_registered(message)
    data = await state.get_data()
    trade_chat_id = data.get("trade_chat_id")
    trade_message_id = data.get("trade_message_id")
    bet = parse_bet(message.text)
    if bet is None:
        if trade_chat_id and trade_message_id:
            await edit_message_text_safe(
                message.bot,
                trade_chat_id,
                trade_message_id,
                tr(user.language, "bet_invalid"),
                reply_markup=trade_menu(user.language, user.last_trade_bet, user.last_trade_direction),
            )
        else:
            await message.answer(tr(user.language, "bet_invalid"))
        return
    if not ensure_balance(user.user_id, bet):
        await state.clear()
        if trade_chat_id and trade_message_id:
            await edit_message_text_safe(
                message.bot,
                trade_chat_id,
                trade_message_id,
                tr(user.language, "trade_no_money"),
                reply_markup=trade_menu(user.language, user.last_trade_bet, user.last_trade_direction),
            )
        else:
            await send_main_menu(message, tr(user.language, "trade_no_money"), user.language)
        return

    direction = data["direction"]
    text = build_trade_result_text(user, direction, bet)
    updated_user = storage.get_or_create_user(
        message.from_user.id,
        message.from_user.username,
        message.from_user.full_name,
        "ru",
    )

    await state.clear()
    if trade_chat_id and trade_message_id:
        await edit_message_text_safe(
            message.bot,
            trade_chat_id,
            trade_message_id,
            text,
            reply_markup=trade_menu(
                updated_user.language,
                updated_user.last_trade_bet,
                updated_user.last_trade_direction,
            ),
        )
        try:
            await message.delete()
        except TelegramBadRequest:
            pass
    else:
        await send_main_menu(message, text, updated_user.language)


@dp.message(F.text == "🎡 Рулетка")
@dp.message(F.text == "🎡 Roulette")
async def open_roulette(message: Message, state: FSMContext) -> None:
    await state.clear()
    user = ensure_registered(message)
    await message.answer(
        tr(user.language, "roulette_open"),
        reply_markup=roulette_menu(user.language, user.last_roulette_bet, user.last_roulette_color),
    )


@dp.callback_query(F.data.startswith("roulette:"))
async def choose_roulette(callback: CallbackQuery, state: FSMContext) -> None:
    user = storage.get_or_create_user(
        callback.from_user.id,
        callback.from_user.username,
        callback.from_user.full_name,
        "ru",
    )
    action = callback.data.split(":")[1]
    if action == "repeat":
        if not user.last_roulette_bet or not user.last_roulette_color:
            await callback.answer(tr(user.language, "roulette_repeat_missing"), show_alert=True)
            return
        if not ensure_balance(user.user_id, user.last_roulette_bet):
            await callback.answer(tr(user.language, "roulette_no_money"), show_alert=True)
            return
        text = build_roulette_result_text(user, user.last_roulette_color, user.last_roulette_bet)
        updated_user = storage.get_or_create_user(
            callback.from_user.id,
            callback.from_user.username,
            callback.from_user.full_name,
            "ru",
        )
        await callback.message.edit_text(
            text,
            reply_markup=roulette_menu(
                updated_user.language,
                updated_user.last_roulette_bet,
                updated_user.last_roulette_color,
            ),
        )
        await state.clear()
        await callback.answer()
        return

    color = action
    await state.set_state(BetStates.roulette_amount)
    await state.update_data(
        color=color,
        roulette_chat_id=callback.message.chat.id,
        roulette_message_id=callback.message.message_id,
    )
    await callback.message.edit_text(
        tr(user.language, "roulette_waiting_amount", color=roulette_label(user.language, color)),
        reply_markup=roulette_menu(user.language, user.last_roulette_bet, user.last_roulette_color),
    )
    await callback.answer()


@dp.message(BetStates.roulette_amount)
async def process_roulette_bet(message: Message, state: FSMContext) -> None:
    user = ensure_registered(message)
    data = await state.get_data()
    roulette_chat_id = data.get("roulette_chat_id")
    roulette_message_id = data.get("roulette_message_id")
    bet = parse_bet(message.text)
    if bet is None:
        if roulette_chat_id and roulette_message_id:
            await edit_message_text_safe(
                message.bot,
                roulette_chat_id,
                roulette_message_id,
                tr(user.language, "bet_invalid_plain"),
                reply_markup=roulette_menu(user.language, user.last_roulette_bet, user.last_roulette_color),
            )
        else:
            await message.answer(tr(user.language, "bet_invalid_plain"))
        return
    if not ensure_balance(user.user_id, bet):
        await state.clear()
        if roulette_chat_id and roulette_message_id:
            await edit_message_text_safe(
                message.bot,
                roulette_chat_id,
                roulette_message_id,
                tr(user.language, "roulette_no_money"),
                reply_markup=roulette_menu(user.language, user.last_roulette_bet, user.last_roulette_color),
            )
        else:
            await send_main_menu(message, tr(user.language, "roulette_no_money"), user.language)
        return

    color = data["color"]
    text = build_roulette_result_text(user, color, bet)
    updated_user = storage.get_or_create_user(
        message.from_user.id,
        message.from_user.username,
        message.from_user.full_name,
        "ru",
    )

    await state.clear()
    if roulette_chat_id and roulette_message_id:
        await edit_message_text_safe(
            message.bot,
            roulette_chat_id,
            roulette_message_id,
            text,
            reply_markup=roulette_menu(
                updated_user.language,
                updated_user.last_roulette_bet,
                updated_user.last_roulette_color,
            ),
        )
        try:
            await message.delete()
        except TelegramBadRequest:
            pass
    else:
        await send_main_menu(message, text, updated_user.language)


@dp.message(F.text == "💣 Мины")
@dp.message(F.text == "💣 Mines")
async def open_mines(message: Message, state: FSMContext) -> None:
    await state.clear()
    user = ensure_registered(message)
    panel_chat_id, panel_message_id = await upsert_mines_panel(
        message,
        user,
        tr(user.language, "mines_open"),
        mines_start_menu(user.language),
    )
    await state.update_data(
        mines_chat_id=panel_chat_id,
        mines_message_id=panel_message_id,
    )


@dp.callback_query(F.data.startswith("mines:mode:"))
async def prepare_mines(callback: CallbackQuery, state: FSMContext) -> None:
    user = storage.get_or_create_user(
        callback.from_user.id,
        callback.from_user.username,
        callback.from_user.full_name,
        "ru",
    )
    mine_count = int(callback.data.split(":")[2])
    grid_size = mines_grid_size(mine_count)
    await state.set_state(BetStates.mines_amount)
    await state.update_data(
        mine_count=mine_count,
        grid_size=grid_size,
        mines_chat_id=callback.message.chat.id,
        mines_message_id=callback.message.message_id,
    )
    await callback.message.edit_text(
        tr(
            user.language,
            "mines_mode_selected",
            mines=mine_count,
            grid=grid_size,
            reward=mines_reward_percent(mine_count),
        ),
        reply_markup=mines_start_menu(user.language),
    )
    await callback.answer()


@dp.message(BetStates.mines_amount)
async def start_mines(message: Message, state: FSMContext) -> None:
    user = ensure_registered(message)
    data = await state.get_data()
    mine_count = int(data.get("mine_count", 1))
    grid_size = int(data.get("grid_size", mines_grid_size(mine_count)))
    mines_chat_id = data.get("mines_chat_id")
    mines_message_id = data.get("mines_message_id")
    bet = parse_bet(message.text)
    if bet is None:
        if mines_chat_id and mines_message_id:
            await edit_message_text_safe(
                message.bot,
                mines_chat_id,
                mines_message_id,
                tr(
                    user.language,
                    "mines_mode_selected",
                    mines=mine_count,
                    grid=grid_size,
                    reward=mines_reward_percent(mine_count),
                )
                + "\n\n"
                + tr(user.language, "bet_invalid_plain"),
                reply_markup=mines_start_menu(user.language),
            )
            try:
                await message.delete()
            except TelegramBadRequest:
                pass
        else:
            await message.answer(tr(user.language, "bet_invalid_plain"))
        return
    if not ensure_balance(user.user_id, bet):
        if mines_chat_id and mines_message_id:
            await edit_message_text_safe(
                message.bot,
                mines_chat_id,
                mines_message_id,
                tr(
                    user.language,
                    "mines_mode_selected",
                    mines=mine_count,
                    grid=grid_size,
                    reward=mines_reward_percent(mine_count),
                )
                + "\n\n"
                + tr(user.language, "mines_no_money"),
                reply_markup=mines_start_menu(user.language),
            )
            try:
                await message.delete()
            except TelegramBadRequest:
                pass
        else:
            await send_main_menu(message, tr(user.language, "mines_no_money"), user.language)
        return

    storage.update_balance(user.user_id, -bet)
    total_cells = grid_size * grid_size
    mine_cells = random.sample(range(total_cells), mine_count)
    safe_cells = [cell for cell in range(total_cells) if cell not in mine_cells]
    storage.save_mines_game(user.user_id, bet, safe_cells, [], mine_cells, grid_size, mine_count)
    await state.clear()
    game_text = tr(
        user.language,
        "mines_started",
        mines=mine_count,
        grid=grid_size,
        reward=mines_reward_percent(mine_count),
    )
    game_markup = mines_grid(storage.get_mines_game(user.user_id), user.language)
    if mines_chat_id and mines_message_id:
        await edit_message_text_safe(
            message.bot,
            mines_chat_id,
            mines_message_id,
            game_text,
            reply_markup=game_markup,
        )
        storage.set_last_mines_message_id(user.user_id, mines_message_id)
        try:
            await message.delete()
        except TelegramBadRequest:
            pass
    else:
        panel = await message.answer(game_text, reply_markup=game_markup)
        storage.set_last_mines_message_id(user.user_id, panel.message_id)


@dp.callback_query(F.data.startswith("mines:open:"))
async def open_mines_cell(callback: CallbackQuery) -> None:
    user_id = callback.from_user.id
    user = storage.get_or_create_user(
        user_id,
        callback.from_user.username,
        callback.from_user.full_name,
        "ru",
    )
    game = storage.get_mines_game(user_id)
    if game is None:
        await callback.answer(tr(user.language, "mines_not_started"), show_alert=True)
        return

    cell = int(callback.data.split(":")[2])
    if cell in game["opened_cells"]:
        await callback.answer(tr(user.language, "mines_cell_opened"))
        return

    if cell in game["mine_cells"]:
        storage.delete_mines_game(user_id)
        await callback.message.edit_text(
            f"{tr(user.language, 'mines_boom')}\n\n{tr(user.language, 'mines_open')}",
            reply_markup=mines_start_menu(user.language),
        )
        await callback.answer()
        return

    game["opened_cells"].append(cell)
    storage.save_mines_game(
        user_id,
        game["bet"],
        game["safe_cells"],
        game["opened_cells"],
        game["mine_cells"],
        game["grid_size"],
        game["mine_count"],
    )
    reward_percent = mines_reward_percent(game["mine_count"])
    current_reward = game["bet"] + (game["bet"] * reward_percent * len(game["opened_cells"])) // 100
    await callback.message.edit_text(
        tr(
            user.language,
            "mines_safe",
            opened=len(game["opened_cells"]),
            mines=game["mine_count"],
            reward=fmt_amount(current_reward, user.language),
        ),
        reply_markup=mines_grid(game, user.language),
    )
    await callback.answer()


@dp.callback_query(F.data == "mines:cashout")
async def cashout_mines(callback: CallbackQuery) -> None:
    user_id = callback.from_user.id
    user = storage.get_or_create_user(
        user_id,
        callback.from_user.username,
        callback.from_user.full_name,
        "ru",
    )
    game = storage.get_mines_game(user_id)
    if game is None:
        await callback.answer(tr(user.language, "mines_cashout_missing"), show_alert=True)
        return

    reward_percent = mines_reward_percent(game["mine_count"])
    reward = game["bet"] + (game["bet"] * reward_percent * len(game["opened_cells"])) // 100
    balance = storage.update_balance(user_id, reward)
    storage.delete_mines_game(user_id)
    await callback.message.edit_text(
        tr(
            user.language,
            "mines_cashout_done",
            reward=fmt_amount(reward, user.language),
            balance=fmt_amount(balance, user.language),
        )
        + "\n\n"
        + tr(user.language, "mines_open"),
        reply_markup=mines_start_menu(user.language),
    )
    await callback.answer()


@dp.message(F.text == "🛠 Работа")
@dp.message(F.text == "🛠 Work")
async def open_work(message: Message, state: FSMContext) -> None:
    await state.clear()
    user = ensure_registered(message)
    await message.answer(
        tr(user.language, "work_open"),
        reply_markup=work_menu(user.language),
    )


@dp.callback_query(F.data == "work:loader")
async def work_loader(callback: CallbackQuery) -> None:
    user = storage.get_or_create_user(
        callback.from_user.id,
        callback.from_user.username,
        callback.from_user.full_name,
        "ru",
    )
    can_work, remaining = storage.can_work(user.user_id)
    if not can_work:
        await callback.message.edit_text(
            tr(user.language, "work_loader_unavailable", remaining=format_remaining(remaining, user.language)),
            reply_markup=work_menu(user.language),
        )
        await callback.answer()
        return

    storage.set_work_progress(user.user_id, 0)
    await callback.message.edit_text(
        tr(user.language, "work_loader_intro", reward=fmt_amount(WORK_REWARD, user.language)),
        reply_markup=loader_button(user.language),
    )
    await callback.answer()


@dp.callback_query(F.data == "work:loader:do")
async def do_loader_work(callback: CallbackQuery) -> None:
    user = storage.get_or_create_user(
        callback.from_user.id,
        callback.from_user.username,
        callback.from_user.full_name,
        "ru",
    )
    can_work, remaining = storage.can_work(user.user_id)
    if not can_work:
        await callback.message.edit_text(
            tr(user.language, "work_shift_closed", remaining=format_remaining(remaining, user.language)),
            reply_markup=work_menu(user.language),
        )
        await callback.answer()
        return

    progress = user.work_progress + 1
    if progress >= 3:
        balance = storage.complete_work(user.user_id)
        await callback.message.edit_text(
            tr(
                user.language,
                "work_done",
                reward=fmt_amount(WORK_REWARD, user.language),
                balance=fmt_amount(balance, user.language),
            ),
        )
    else:
        storage.set_work_progress(user.user_id, progress)
        await callback.message.edit_text(
            tr(user.language, "work_progress", progress=progress),
            reply_markup=loader_button(user.language),
        )
    await callback.answer()


@dp.message()
async def fallback(message: Message) -> None:
    user = ensure_registered(message)
    await send_main_menu(
        message,
        tr(user.language, "fallback"),
        user.language,
    )


async def main() -> None:
    load_env()
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("Не найден BOT_TOKEN в .env или переменных окружения")
    global ADMIN_ID
    ADMIN_ID = int(os.getenv("ADMIN_ID", "0") or "0")

    logging.basicConfig(level=logging.INFO)
    bot = Bot(token=token, default=DefaultBotProperties(parse_mode="HTML"))
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
