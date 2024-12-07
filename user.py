import logging
from aiogram import Bot
from aiogram.enums import ParseMode
from config_data.config import Config, load_config

config: Config = load_config('.env')
bot = Bot(token=config.tg_bot.token)

# Настройка логирования
logger = logging.getLogger(__name__)
handler = logging.FileHandler("user.log")
formatter = logging.Formatter("%(filename)s:%(lineno)d #%(levelname)-8s [%(asctime)s] - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


async def message_bybit_binance(tg_id, symbol, liquidation_type, liquidation, price):
    try:
        sml = {'Short': '🟢', 'Long': '🔴'}
        coinglass = f'https://www.coinglass.com/tv/ru/binance_{symbol}'
        bybit = f'https://www.bybit.com/trade/usdt/{symbol}'
        binance = f'https://www.binance.com/ru/futures/{symbol}'

        # Отправка сообщения
        await bot.send_message(
            chat_id=tg_id,
            text=f'🌕Binance\n'
                 f'{sml[liquidation_type]} {liquidation_type} <b>{symbol[0:-4]}</b>\n'
                 f'Liquidation amount: <b>{liquidation} USDT</b>\n'
                 f'Price: {price}\n'
                 f'<a href=\"{binance}\">Binance</a> | <a href=\"{coinglass}\">CoinGlass</a> | <a href=\"{bybit}\">ByBit</a>',
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
        logger.info(f"Сообщение отправлено в Telegram для {symbol} с суммой ликвидации {liquidation} USDT.")
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения в Telegram: {e}")


async def message_binance(tg_id, symbol, liquidation_type, liquidation, price):
    try:
        sml = {'Short': '🟢', 'Long': '🔴'}
        coinglass = f'https://www.coinglass.com/tv/ru/binance_{symbol}'
        binance = f'https://www.binance.com/ru/futures/{symbol}'

        # Отправка сообщения
        await bot.send_message(
            chat_id=tg_id,
            text=f'🌕Binance: {sml[liquidation_type]} {liquidation_type} <b>{symbol[0:-4]}</b>, liquidation amount: {liquidation} USDT price: {price}\n'
                 f'<a href=\"{binance}\">Binance</a> | <a href=\"{coinglass}\">CoinGlass</a>',
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
        logger.info(f"Сообщение отправлено в Telegram для {symbol} с суммой ликвидации {liquidation} USDT.")
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения в Telegram: {e}")