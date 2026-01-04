from google.cloud import bigquery
import pandas as pd
import os

# === ШАГ 1: Указываем путь к service account ключу ===
# ВАЖНО: путь должен быть абсолютным и без опечаток
SERVICE_ACCOUNT_PATH = r"C:\Users\wtmya\Documents\GitHub\google-da-bootcamp\keys\service-account.json"

# Проверяем, что файл существует
if not os.path.exists(SERVICE_ACCOUNT_PATH):
    raise FileNotFoundError(f"Ключ не найден по пути: {SERVICE_ACCOUNT_PATH}")

# Устанавливаем переменную окружения
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

# === ШАГ 2: Подключаемся к BigQuery ===
try:
    client = bigquery.Client()
    print("✅ Успешно подключились к BigQuery")
except Exception as e:
    print(f"❌ Ошибка подключения: {e}")
    exit(1)

# === ШАГ 3: SQL-запрос (исправлена опечатка igquery → bigquery) ===
query = """
SELECT
  fullVisitorId AS user_id,
  geoNetwork.country AS country,
  hit.transaction.transactionRevenue / 1000000 AS revenue
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`,
  UNNEST(hits) AS hit
WHERE
  hit.transaction.transactionRevenue IS NOT NULL
LIMIT 1000
"""

# === ШАГ 4: Выполняем запрос и получаем DataFrame ===
try:
    print("⏳ Выполняю запрос в BigQuery...")
    df = client.query(query).to_dataframe()
    print(f"✅ Получено {len(df)} строк")
except Exception as e:
    print(f"❌ Ошибка выполнения запроса: {e}")
    exit(1)

# === ШАГ 5: Сохраняем в CSV (абсолютный путь) ===
OUTPUT_PATH = r"C:\Users\wtmya\Documents\GitHub\google-da-bootcamp\transactions_20170801.csv"

try:
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Файл сохранен: {OUTPUT_PATH}")
except Exception as e:
    print(f"❌ Ошибка сохранения CSV: {e}")
    exit(1)

# === ШАГ 6: Выводим статистику ===
print(f"\n📊 Итог:")
print(f"  - Строк в DataFrame: {len(df)}")
print(f"  - Столбцы: {list(df.columns)}")
print(f"  - Размер файла: {os.path.getsize(OUTPUT_PATH)} байт")