import asyncio
import json
import ssl
import logging
import websockets
from pybit.unified_trading import HTTP
from user import message_bybit_binance, message_binance
from config_data.config import Config, load_config
import httpx

# Настройка логирования
logger = logging.getLogger(__name__)
handler = logging.FileHandler("binance_main.log")
formatter = logging.Formatter("%(filename)s:%(lineno)d #%(levelname)-8s [%(asctime)s] - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Лимит суммы ликвидации для уведомлений
LIQUIDATION_LIMIT = 15000
TOP_50_BINANCE = []  # Список топ-50 торговых пар Binance
bybit_symbol = []  # Список всех фьючерсных монет Bybit

config: Config = load_config('.env')
session = HTTP(
    testnet=False,
    api_key=config.by_bit.api_key,
    api_secret=config.by_bit.api_secret,
)


# Функция для получения топ-50 торговых пар с Binance
async def fetch_top_50_binance():
    try:
        url = "https://api.binance.com/api/v3/ticker/24hr"
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            data = response.json()

            # Отбор торговых пар с USDT и сортировка по объему
            usdt_pairs = [
                {
                    "symbol": ticker["symbol"],
                    "volume": float(ticker["quoteVolume"])
                }
                for ticker in data if ticker["symbol"].endswith("USDT")
            ]
            sorted_pairs = sorted(usdt_pairs, key=lambda x: x["volume"], reverse=True)
            global TOP_50_BINANCE
            TOP_50_BINANCE = [pair["symbol"] for pair in sorted_pairs[:50]]
            logger.info(f"Обновлен топ-50 Binance: {TOP_50_BINANCE}")
    except Exception as e:
        logger.error(f"Ошибка при получении топ-50 Binance: {e}")


# Функция для получения всех фьючерсных монет Bybit
async def fetch_bybit_symbols():
    try:
        global bybit_symbol
        data_bybit = session.get_tickers(category="linear")
        bybit_symbol = [
            dicts['symbol'] for dicts in data_bybit['result']['list']
            if 'USDT' in dicts['symbol']
        ]
        logger.info(f"Обновлен список всех монет Bybit: {bybit_symbol}")
    except Exception as e:
        logger.error(f"Ошибка при получении списка монет Bybit: {e}")

# Функция для обновления топ-50 Binance и списка монет Bybit каждые 24 часа
async def update_symbols():
    while True:
        try:
            logger.info("Обновление данных Binance и Bybit...")
            await fetch_top_50_binance()
            await fetch_bybit_symbols()
            logger.info("Данные Binance и Bybit успешно обновлены.")
        except Exception as e:
            logger.error(f"Ошибка при обновлении данных: {e}")
        await asyncio.sleep(24 * 60 * 60)  # Обновление каждые 24 часа


async def on_message(message):
    try:
        data = json.loads(message)
        if "o" in data:  # Проверяем, есть ли информация о ликвидации
            order = data["o"]
            symbol = order["s"]  # Символ, например, BTCUSDT
            side = order["S"]  # Тип ордера: "BUY" или "SELL"
            qty = float(order["q"])  # Количество монет
            price = float(order["p"])  # Цена ликвидации
            notional = qty * price  # Сумма ликвидации в USDT
            if notional >= LIQUIDATION_LIMIT and symbol not in TOP_50_BINANCE:
                liquidation_type = "Short" if side == "BUY" else "Long"
                if symbol not in bybit_symbol:
                    await message_binance(-1002304776308, symbol, liquidation_type, f'{notional:.2f}', price)
                else:
                    await message_bybit_binance(-1002304776308, symbol, liquidation_type, f'{notional:.2f}', price)
                logger.info(f"Обработано событие ликвидации: {symbol}, Тип: {liquidation_type}, Сумма: {notional:.2f} USDT")
    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения WebSocket: {e}")


async def on_error(error):
    logger.error(f"Ошибка WebSocket: {error}")


async def on_close(close_status_code, close_msg):
    logger.warning(f"Соединение закрыто. Код: {close_status_code}, Сообщение: {close_msg}")


async def on_open(ws):
    try:
        params = {
            "method": "SUBSCRIBE",
            "params": ["!forceOrder@arr"],  # Подписка на ликвидации
            "id": 1
        }
        await ws.send(json.dumps(params))
        logger.info("Подписка на ликвидации отправлена.")
    except Exception as e:
        logger.error(f"Ошибка при подписке на ликвидации: {e}")

url = "wss://fstream.binance.com/ws"  # WebSocket Binance для фьючерсов
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

async def main():
    try:
        # Запуск фонового обновления данных
        await asyncio.create_task(update_symbols())

        # Подключение к WebSocket
        async with websockets.connect(url, ssl=ssl_context) as ws:
            logger.info("Соединение с WebSocket Binance открыто.")
            await on_open(ws)

            while True:
                message = await ws.recv()
                await on_message(message)
    except Exception as e:
        logger.critical(f"Критическая ошибка в основном цикле: {e}")

if __name__ == "__main__":
    asyncio.run(main())