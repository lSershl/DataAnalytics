from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd
import time

# Задаём настройки подключения к Postgres
# Пример: 'postgresql://username:password@hostaddress:5432/your_database'
engine = create_engine('postgresql://postgres:postgres@localhost:5450/telecom_db')

# Начинаем отсчёт времени выполнения скрипта
start_time = time.time()

# ! ВНИМАНИЕ ! Убедитесь, что был выполнен предыдущий скрипт import_from_csv_to_postgres.py

with engine.begin() as pg_conn:

    # Заменяем пропуске в таблице из схемы stg и копируем в новую таблицу в схеме prod
    pg_conn.execute(text(
        """
        CREATE TABLE IF NOT EXISTS prod.customer_data_refined AS
            (
                SELECT 
                    customer_id,
                    gender,
                    age,
                    married,
                    state,
                    number_of_referrals,
                    tenure_in_months,
                    COALESCE(value_deal, 'None') AS value_deal,
                    phone_service,
                    COALESCE(multiple_lines, 'No') As multiple_lines,
                    internet_service,
                    COALESCE(internet_type, 'None') AS internet_type,
                    COALESCE(online_security, 'No') AS online_security,
                    COALESCE(online_backup, 'No') AS online_backup,
                    COALESCE(device_protection_plan, 'No') AS device_protection_plan,
                    COALESCE(premium_support, 'No') AS premium_support,
                    COALESCE(streaming_tv, 'No') AS streaming_tv,
                    COALESCE(streaming_movies, 'No') AS streaming_movies,
                    COALESCE(streaming_music, 'No') AS streaming_music,
                    COALESCE(unlimited_data, 'No') AS unlimited_data,
                    contract,
                    paperless_billing,
                    payment_method,
                    monthly_charge,
                    total_charges,
                    total_refunds,
                    total_extra_data_charges,
                    total_long_distance_charges,
                    total_revenue,
                    customer_status,
                    COALESCE(churn_category, 'Others') AS churn_category,
                    COALESCE(churn_reason , 'Others') AS churn_reason
                FROM stg.customer_data
            )
        """
    ))

    # Создаём представления на основе таблицы из схемы prod и помещаем их в схему dm
    pg_conn.execute(text(
        """
        CREATE OR REPLACE VIEW dm.vw_churn_data AS
            SELECT * 
            FROM prod.customer_data_refined 
            WHERE customer_status IN ('Churned', 'Stayed');

        CREATE OR REPLACE VIEW dm.vw_join_data AS
            SELECT * 
            FROM prod.customer_data_refined 
            WHERE customer_status = 'Joined';
        """
    ))

# Передаём представления в датафрейм и сохраняем их в csv файлы
df_view_data = pd.read_sql(text(
    """
    SELECT *
    FROM dm.vw_churn_data
    """),
    con=engine
)

df_view_data.to_csv('../data/vw_churn_data.csv', index=False)

df_view_data = pd.read_sql(text(
    """
    SELECT *
    FROM dm.vw_join_data
    """),
    con=engine
)

df_view_data.to_csv('../data/vw_join_data.csv', index=False)

# Выводим время выполнения в терминал после завершения работы скрипта
end_time = time.time()
total_time = round(end_time - start_time, 2)
print(f"Готово. Время выполнения: {total_time} секунд(ы)")