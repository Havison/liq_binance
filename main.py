import asyncio
import json
import ssl
import logging
import websockets
from pybit.unified_trading import HTTP
from user import message_bybit_binance, message_binance
from config_data.config import Config, load_config

# Настройка логирования
logger = logging.getLogger(__name__)
handler = logging.FileHandler("binance_main.log")
formatter = logging.Formatter("%(filename)s:%(lineno)d #%(levelname)-8s [%(asctime)s] - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


# Лимит суммы ликвидации для уведомлений
LIQUIDATION_LIMIT = 15000
TOP_50 = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'SOLUSDT', 'XRPUSDT', 'DOTUSDT', 'DOGEUSDT', 'AVAXUSDT', 'LUNAUSDT', 'SHIBUSDT', 'LINKUSDT', 'LTCUSDT', 'ALGOUSDT', 'MATICUSDT', 'TRXUSDT', 'XLMUSDT', 'VETUSDT', 'ATOMUSDT', 'FILUSDT', 'ETCUSDT', 'ICPUSDT', 'NEOUSDT', 'BCHUSDT', 'XTZUSDT', 'THETAUSDT', 'MANAUSDT', 'QNTUSDT', 'SUSHIUSDT', 'SANDUSDT', 'DCRUSDT', 'KSMUSDT', 'DASHUSDT', 'YFIUSDT', 'BATUSDT', 'WAVESUSDT', 'ZRXUSDT', 'RENUSDT', 'UNIUSDT', 'ICXUSDT', 'AAVEUSDT', 'ARUSDT', 'COMPUSDT', 'MKRUSDT', 'CRVUSDT', 'SNXUSDT', 'AVAXUSDT', 'SCUSDT', 'OMGUSDT', 'USTUSDT']

config: Config = load_config('.env')
session = HTTP(
    testnet=False,
    api_key=config.by_bit.api_key,
    api_secret=config.by_bit.api_secret,
)
data_bybit = session.get_tickers(category="linear")
bybit_symbol = [dicts['symbol'] for dicts in data_bybit['result']['list']
                if 'USDT' in dicts['symbol'] and dicts['symbol'] not in TOP_50]


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

            if notional >= LIQUIDATION_LIMIT and symbol not in TOP_50:
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
