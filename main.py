import asyncio
import time
from pathlib import Path

# import requests
import os
import re
import json
import base64
import sqlite3

from pytonlib.utils.common import b64str_to_bytes
# from TonTools.Contracts.Contract import Transaction
# from pytonlib import TonlibClient
from pytonlib.utils.tlb import Transaction, Slice, Cell, CommentMessage, JettonTransferNotificationMessage
# from tonsdk.utils import b64str_to_bytes
from TonTools.Providers.TonCenterClient import GetMethodError
from fastapi import FastAPI
from pydantic import BaseModel
# from pytonlib.utils.common import b64str_to_bytes
# from sqlalchemy.sql.elements import Slice
from tonsdk.contract.wallet import Wallets, WalletVersionEnum
from datetime import datetime, timedelta
from TonTools import TonCenterClient, Wallet
from tvm_valuetypes import deserialize_boc

WALLETS_DIR = "wallets"
TOTAL_WALLETS = 5

monitoring_started = False

# здесь прописывается основной кошелек для перевода
MASTER_WALLET_ADDRESS = "UQBRPQfPgn4rv5YnmEltwodj17q3r9QPc7aey4fXah0mHfrZ"
external_api_url = "https://example.com/notify"

app = FastAPI()

# Соединение с базой данных
conn = sqlite3.connect('wallets.db', check_same_thread=False)
cursor = conn.cursor()

# Создание таблиц, если они не существуют
cursor.execute('''CREATE TABLE IF NOT EXISTS wallets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    address TEXT,
                    mnemonic TEXT,
                    active BOOLEAN DEFAULT 0,
                    user_id TEXT,
                    balance REAL DEFAULT 0,
                    amount_api REAL DEFAULT 0
                )''')

cursor.execute('''CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT,
                    wallet_address TEXT,
                    amount REAL,
                    activation_time TEXT,
                    paid BOOLEAN DEFAULT 0,
                    payment_time TEXT
                )''')
conn.commit()

# async def get_client():
#     url = 'https://ton.org/global-config.json'
#
#     config = requests.get(url).json()
#
#     keystore_dir = '/wallets/1'
#     Path(keystore_dir).mkdir(parents=True, exist_ok=True)
#
#     client = TonlibClient(ls_index=2, config=config, keystore=keystore_dir, tonlib_timeout=20)
#
#     await client.init()
#
#     return client


class TransactionRequest(BaseModel):
    user_id: str
    amount: float


async def check_wallet_balances_periodically():
    while True:
        await check_wallet_balances()  # Ваша функция проверки балансов
        await asyncio.sleep(7200)  # Ожидание 6 часов (21600 секунд)


async def ensure_wallets_exist():
    # Создаем директорию, если она не существует
    if not os.path.exists(WALLETS_DIR):
        os.makedirs(WALLETS_DIR)

    # Очищаем таблицу кошельков перед загрузкой данных
    cursor.execute('DELETE FROM wallets')
    conn.commit()  # Фиксируем удаление всех записей

    # Получаем список файлов в директории
    wallet_files = sorted([f for f in os.listdir(WALLETS_DIR) if f.isdigit()])
    existing_wallets_count = len(wallet_files)

    if existing_wallets_count < TOTAL_WALLETS:
        # Определяем номер для нового кошелька
        last_wallet_number = int(wallet_files[-1]) if wallet_files else 0

        for wallet_number in range(last_wallet_number + 1, TOTAL_WALLETS + 1):
            wallet_data = await create_wallet()  # Создаем новый кошелек

            # Преобразуем все байтовые значения в строковый формат (например, base64)
            wallet_data_serializable = {}
            for key, value in wallet_data.items():
                if isinstance(value, bytes):
                    wallet_data_serializable[key] = base64.b64encode(value).decode('utf-8')
                else:
                    wallet_data_serializable[key] = value

            # Записываем данные кошелька в файл
            wallet_file_path = os.path.join(WALLETS_DIR, str(wallet_number))
            with open(wallet_file_path, 'w') as wallet_file:
                json.dump(wallet_data_serializable, wallet_file)

    wallet_files1 = sorted([f for f in os.listdir(WALLETS_DIR) if f.isdigit()])

    for wallet_file in wallet_files1:
        wallet_file_path = os.path.join(WALLETS_DIR, wallet_file)
        try:
            with open(wallet_file_path, 'r') as file:
                wallet_data = json.load(file)

            cursor.execute('''
                  INSERT OR IGNORE INTO wallets (address, mnemonic, active) VALUES (?, ?, ?)
                  ''', (wallet_data['address'], ' '.join(wallet_data['seed_phrase']), False))
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Ошибка при чтении кошелька {wallet_file}: {str(e)}")
        continue  # Пропускаем файл с ошибками

    conn.commit()  # Фиксируем изменения после переноса существующих кошелько


async def create_wallet():
    mnemonic, pub_k, priv_k, wallet = Wallets.create(version=WalletVersionEnum.v4r1, workchain=0)
    # mnemonic = mnemonic_new()
    # wallet = Wallet(mnemonic, Wallet.VERSION)
    print(mnemonic)
    wallet_address = wallet.address.to_string(True, True, True)
    print(wallet_address)

    return {
        "address": wallet_address,
        "seed_phrase": mnemonic,
        "pub_k": pub_k,
        "priv_k": priv_k
    }


# Функция для проверки балансов и перевода
async def check_wallet_balances():
    cursor.execute('SELECT id, address, mnemonic,balance FROM wallets')
    wallets = cursor.fetchall()

    provider = TonCenterClient(testnet=True)
    print('----------------------------------------------------------------------------')

    for wallet in wallets:
        wallet_id, wallet_address, mnemonic, balance = wallet

        mnemonic_array = mnemonic.split()
        await asyncio.sleep(8)
        wallet = Wallet(mnemonics=mnemonic_array, version='v3r2', provider=provider)
        # Получение баланса кошелька
        wallet_nano_balance = await wallet.get_balance()
        await asyncio.sleep(5)
        # if wallet_address == 'kQA3jzHPEYbEWuCSZQ2Uz-qihPxbBXBm4ppHD8FJk6vwGwNS':
        #     await wallet.deploy()
        #     print('sss --11--- sss')

        if wallet_nano_balance > 0:
            wallet_state = await wallet.get_state()
            await asyncio.sleep(5)

            wallet_balance = wallet_nano_balance / 1000000000
            print(f"Баланс кошелька {wallet_address}: {wallet_balance} TON статус кошелька {wallet_state}")

            if wallet_state == 'uninitialized':
                await wallet.deploy()
                await asyncio.sleep(15)
                wallet_state = await wallet.get_state()
                await asyncio.sleep(1)
                print(wallet_state)

            if wallet_address == MASTER_WALLET_ADDRESS:
                # print('sss ----- sss')
                continue

            if wallet_balance - 0.5 > 0.5 and wallet_state == 'active':
                try:
                    # Трансфер средств на мастер-кошелек
                    await wallet.transfer_ton(destination_address=MASTER_WALLET_ADDRESS, amount=wallet_balance - 0.5,
                                              message='test')
                    print(f"Переведено {wallet_balance - 0.5} TON с кошелька {wallet_address} на мастер-кошелек.")

                except GetMethodError as e:
                    print(f"Ошибка при получении seqno для кошелька {wallet_address}: {str(e)}")
                    continue  # Пропускаем этот кошелек и идем к следующему

                except (KeyError, json.JSONDecodeError) as e:
                    print(f"Ошибка при чтении баланса кошелька {wallet_address}: {str(e)}")
                    continue

                except Exception as e:
                    print(f"Неизвестная ошибка при обработке кошелька {wallet_address}: {str(e)}")
                    continue


# Эндпоинт для проверки статуса оплаты
@app.get("/check_transactions")
async def check_payment_status():
    provider = TonCenterClient()
    wallet = Wallet(provider=provider, address=MASTER_WALLET_ADDRESS)
    # client = await get_client()

    try:
        await asyncio.sleep(5)
        trs = await wallet.get_transactions(limit=50)

    except GetMethodError as e:
        print(f"Ошибка при получении seqno для кошелька {MASTER_WALLET_ADDRESS}: {str(e)}")

    except (KeyError, json.JSONDecodeError) as e:
        print(f"Ошибка при чтении баланса кошелька {MASTER_WALLET_ADDRESS}: {str(e)}")

    except Exception as e:
        print(f"Неизвестная ошибка : {str(e)}")

    filtered_transactions = []

    if trs:
        # print(trs[0].to_dict())
        # print('---------------------')
        # cell = deserialize_boc(b64str_to_bytes(trs[0].to_dict()['in_msg'].get("msg_data")))
        # tr_data = JettonTransferNotificationMessage(Slice(cell))
        # re = tr_data.amount/1000000
        # print(re)


        for tr in trs:

            if tr.to_dict_user_friendly()["type"] == 'in':
                memo = ''
                is_base64, decoded_bytes = is_base64_encoded(tr.to_dict()["in_msg"].get("msg_data"))

                if is_base64:
                    memo = decoded_bytes

                dt_object = datetime.utcfromtimestamp(tr.to_dict()["utime"])
                formatted_time = dt_object.strftime('%H:%M %d-%m-%Y')

                cell = deserialize_boc(b64str_to_bytes(tr.to_dict()['in_msg'].get("msg_data")))
                tr_data = JettonTransferNotificationMessage(Slice(cell))
                re = tr_data.amount / 1000000

                filtered_transactions.append({
                    "status": tr.to_dict_user_friendly()["status"],
                    "time": formatted_time,
                    "hash": tr.to_dict()["hash"],
                    "memo": memo,
                    "value": re
                })
        return {"return": filtered_transactions}

    return {"status": "0"}


def is_base64_encoded(data):
    if len(data) % 4 != 0:
        return False
    try:
        decoded_data = base64.b64decode(data)
        decoded_text = decoded_data.decode('utf-8', errors='ignore')
        match = re.search(r'\b[a-zA-Z0-9]{8}\b', decoded_text)
        # email_match = re.search(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', decoded_text)
        # print(email_match)

        if match:
            return True, match.group(0)
        else:
            return False, None

    except Exception:
        return False, None



@app.get("/check-payment-status")
async def check_payment_status(user_id: str):
    now = datetime.now()
    one_hour_ago = now - timedelta(hours=1)

    # Проверяем кошельки, у которых активность True
    cursor.execute('SELECT id, address, mnemonic, user_id, amount_api FROM wallets WHERE active = 1')
    active_wallets = cursor.fetchall()

    # Ищем запись с указанным user_id и статусом оплаты
    cursor.execute('''
        SELECT payment_time, paid, activation_time
        FROM logs 
        WHERE user_id = ?
        ORDER BY activation_time DESC LIMIT 1
    ''', (user_id,))
    record = cursor.fetchone()
    payment_record, paid, activation_time = record

    if record:
        print(record)
        if payment_record:
            payment_time = datetime.strptime(payment_record, "%Y-%m-%d %H:%M:%S.%f")
            print("payment_time")
            if payment_time > one_hour_ago and paid == True:
                return {"status": "Payment confirmed", "payment_time": payment_time}
            else:
                return {"status": "Payment confirmed but older than one hour", "payment_time": payment_time}
        else:
            return {"status": "No payment found or payment not confirmed"}
    else:
        return {"status": "No payment"}


# Регистрация функции как обработчика события startup
# @app.on_event("startup")
# async def startup_event():
#     asyncio.create_task(check_wallet_balances_periodically())
#     await ensure_wallets_exist()


# Эндпоинт для создания транзакции
@app.post("/create-transaction")
async def create_transaction(request: TransactionRequest):
    global monitoring_started

    now = datetime.now()
    one_hour_ago = now - timedelta(hours=1)

    # Получаем свободный кошелек
    # cursor.execute('SELECT id, address FROM wallets WHERE active = 0 LIMIT 1')
    # wallet = cursor.fetchone()

    # Получаем свободный кошелек без транзакций за последний час из таблицы логов
    cursor.execute('''
        SELECT w.id, w.address 
        FROM wallets w
        LEFT JOIN logs l ON w.address = l.wallet_address 
        WHERE w.active = 0 
        GROUP BY w.id
        HAVING MAX(l.activation_time) IS NULL OR MAX(l.activation_time) < ?
        LIMIT 1
    ''', (one_hour_ago,))
    wallet = cursor.fetchone()

    if not wallet:
        return {"error": "No available wallets"}

    wallet_id, wallet_address = wallet

    # Активируем кошелек
    cursor.execute('UPDATE wallets SET active = 1, user_id = ?, amount_api = ? WHERE id = ?',
                   (request.user_id, request.amount, wallet_id))
    conn.commit()
    # Создаем запись в логах
    cursor.execute('INSERT INTO logs (user_id, wallet_address, amount, activation_time) VALUES (?, ?, ?, ?)',
                   (request.user_id, wallet_address, request.amount, datetime.now()))
    conn.commit()

    if not monitoring_started:
        monitoring_started = True  # Обновляем статус мониторинга
        # thread = threading.Thread(target=check_transactions, daemon=True)
        # thread.start()
        asyncio.create_task(check_transactions())

    # Возвращаем адрес кошелька клиенту
    return {"wallet_address": wallet_address}


# Фоновая задача для проверки транзакций
async def check_transactions():
    global monitoring_started

    while True:
        # Проверяем наличие активных кошельков
        cursor.execute('SELECT COUNT(*) FROM wallets WHERE active = 1')
        active_wallets_count = cursor.fetchone()[0]

        if active_wallets_count > 0:
            print("Активные кошельки обнаружены, продолжаем мониторинг...")

            # Проверяем кошельки, у которых активность True
            cursor.execute('SELECT id, address, mnemonic, user_id, amount_api FROM wallets WHERE active = 1')
            active_wallets = cursor.fetchall()

            for wallet in active_wallets:
                wallet_id, wallet_address, mnemonic, user_id, amount_api = wallet

                new_balance = await get_wallet_balance(wallet_address, mnemonic)

                # # Проверяем, оплачено ли уже
                # cursor.execute('SELECT paid FROM logs WHERE wallet_address = ? ORDER BY activation_time DESC LIMIT 1',
                #                (wallet_address,))
                # paid_status = cursor.fetchone()[0]

                # if paid_status:  # Если уже оплачено, пропускаем 1000000000
                #     print(f"Кошелек {wallet_address} уже был оплачен.")
                #     continue

                if new_balance * 1000000000 >= amount_api:
                    # Уведомляем стороннее API о транзакции
                    # notify_external_api(user_id, new_balance)

                    # Обновляем статус в логах на "оплачено" и записываем время оплаты
                    print(f"Кошелек {wallet_address} уже был оплачен {user_id}.")
                    cursor.execute('''UPDATE logs 
                                      SET paid = 1, payment_time = ? 
                                      WHERE wallet_address = ? AND user_id = ?''',
                                   (datetime.now(), wallet_address, user_id))
                    conn.commit()

                    # Обновляем баланс в базе данных
                    cursor.execute('UPDATE wallets SET active = 0, user_id = NULL, amount_api = 0 WHERE id = ?', (wallet_id,))
                    conn.commit()
                else:
                    # Если прошло больше 10 часов с момента активации, деактивируем кошелек
                    cursor.execute(
                        'SELECT activation_time FROM logs WHERE wallet_address = ? ORDER BY activation_time DESC LIMIT 1',
                        (wallet_address,))
                    activation_time = cursor.fetchone()[0]
                    activation_time = datetime.strptime(activation_time, "%Y-%m-%d %H:%M:%S.%f")

                    if datetime.now() - activation_time > timedelta(minutes=60):
                        cursor.execute('UPDATE wallets SET active = 0, user_id = NULL, amount_api = 0 WHERE id = ?', (wallet_id,))
                        conn.commit()

            await asyncio.sleep(40)
        else:
            print("Нет активных кошельков, останавливаем мониторинг.")
            monitoring_started = False  # Останавливаем мониторинг
            break


# Функция для получения баланса кошелька
async def get_wallet_balance(wallet_address: str, mnemonic: str) -> float:
    hours_trs_last = 1 * 3600
    provider = TonCenterClient(testnet=True)
    mnemonic_array = mnemonic.split()
    wallet = Wallet(provider=provider, address=wallet_address)

    try:
        await asyncio.sleep(5)
        trs = await wallet.get_transactions(limit=2)
        await asyncio.sleep(5)
        my_wallet_nano_balance = await wallet.get_balance()

    except GetMethodError as e:
        print(f"Ошибка при получении seqno для кошелька {wallet_address}: {str(e)}")

    except (KeyError, json.JSONDecodeError) as e:
        print(f"Ошибка при чтении баланса кошелька {wallet_address}: {str(e)}")

    except Exception as e:
        print(f"Неизвестная ошибка при обработке кошелька {wallet_address}: {str(e)}")

    if trs:
        current_time = time.time()
        trs_time = trs[0].to_dict_user_friendly()['utime']

        if current_time-trs_time <= hours_trs_last:
            print(trs[0].to_dict_user_friendly()['value'])
            print(my_wallet_nano_balance)
            return trs[0].to_dict_user_friendly()['value']
    return 0  # Пример возврата


# Функция для уведомления внешнего API о транзакции
# def notify_external_api(user_id: str, amount: float):
#     # external_api_url = "https://example.com/notify"
#     payload = {"user_id": user_id, "amount": amount, "timestamp": datetime.now().isoformat()}
#     response = requests.post(external_api_url, json=payload)
#     print(f"Notified external API: {response.status_code}")