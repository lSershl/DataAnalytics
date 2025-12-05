from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd
import time

# Задаём настройки подключения к Postgres
# Пример: 'postgresql://username:password@hostaddress:5432/your_database'
engine = create_engine('postgresql://postgres:postgres@localhost:5450/telecom_db')

# Начинаем отсчёт времени выполнения скрипта
start_time = time.time()

# Создаём схемы и таблицы в БД
with engine.begin() as pg_conn:

    # Если ранее уже создавались таблицы и схемы, мы заранее всё удаляем, чтобы случайно не дублировать данные
    pg_conn.execute(text(
        """
        DROP SCHEMA IF EXISTS stg CASCADE;
        DROP SCHEMA IF EXISTS prod CASCADE;
        DROP SCHEMA IF EXISTS dm CASCADE;
        """
    ))

    pg_conn.execute(text(
        """
        CREATE SCHEMA stg;
        CREATE SCHEMA prod;
        CREATE SCHEMA dm;

        CREATE TABLE stg.customer_data (
            customer_id VARCHAR(50) PRIMARY KEY,
            gender VARCHAR(50),
            age INT,
            married VARCHAR(50),
            state VARCHAR(50),
            number_of_referrals INT,
            tenure_in_months INT,
            value_deal VARCHAR(50),
            phone_service VARCHAR(50),
            multiple_lines VARCHAR(50),
            internet_service VARCHAR(50),
            internet_type VARCHAR(50),
            online_security VARCHAR(50),
            online_backup VARCHAR(50),
            device_protection_plan VARCHAR(50),
            premium_support VARCHAR(50),
            streaming_tv VARCHAR(50),
            streaming_movies VARCHAR(50),
            streaming_music VARCHAR(50),
            unlimited_data VARCHAR(50),
            contract VARCHAR(50),
            paperless_billing VARCHAR(50),
            payment_method VARCHAR(50),
            monthly_charge NUMERIC,
            total_charges NUMERIC,
            total_refunds NUMERIC,
            total_extra_data_charges NUMERIC,
            total_long_distance_charges NUMERIC,
            total_revenue NUMERIC,
            customer_status VARCHAR(50),
            churn_category VARCHAR(50),
            churn_reason VARCHAR(50)
        );
        """
    ))

# Открываем csv файл и переносим в датафрейм
df = pd.read_csv("../data/customer_data.csv")

# Приводим названия столбцов датафрейма к нижнему регистру
# В Postgres для названий таблиц и столбцов принято использовать snake case с нижним регистром
df.columns = map(str.lower, df.columns)

df.to_sql(
    name="customer_data", # имя таблицы
    schema='stg', # схема
    con=engine,  # подключение
    if_exists="append", # добавляем данные в заранее подготовленную таблицу
    index=False # без индексов
)

# Выводим время выполнения в терминал после завершения работы скрипта
end_time = time.time()
total_time = round(end_time - start_time, 2)
print(f"Импорт данных завершён. Время выполнения: {total_time} секунд(ы)")