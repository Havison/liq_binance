from dataclasses import dataclass
from environs import Env


@dataclass
class ByBit:
    api_key: str         # ключ от bybit
    api_secret: str          # секрет от ключа bybit


@dataclass
class TgBot:
    token: str # Токен для доступа к телеграм-боту


@dataclass
class Pay:
    api_key_cloud: str
    api_secret_cloud: str


@dataclass
class Database:
    host: str
    user: str
    password: str
    database_type: str
    db_url: str
    db: str


@dataclass
class Config:
    tg_bot: TgBot
    by_bit: ByBit
    binance_key: ByBit
    pay: Pay
    database: Database


def load_config(path: str | None = None) -> Config:

    env: Env = Env()
    env.read_env(path)

    return Config(
        tg_bot=TgBot(
            token=env('BOT_TOKEN'),
        ),
        by_bit=ByBit(
            api_key=env('API_KEY'),
            api_secret=env('API_SECRET'),
        ),
        binance_key=ByBit(
            api_key=env('API_KEY_binance'),
            api_secret=env('API_SECRET_binance')
        ),
        pay=Pay(
            api_key_cloud=env('API_KEY_CLOUD'),
            api_secret_cloud=env('API_SECRET_CLOUD')
        ),
        database=Database(host=env('DATABASE_HOST'),
                          user=env('DATABASE_USER'),
                          password=env('DATABASE_PASSWORD'),
                          database_type=env('DATABASE_TYPE'),
                          db_url=env('DB_URL'),
                          db=env('DB_MARKET')
                          )
    )

