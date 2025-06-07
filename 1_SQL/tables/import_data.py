import pandas as pd
from sqlalchemy import create_engine
import time

tables_list = [
    "users", 
    "orders", 
    "products", 
    "couriers", 
    "user_actions", 
    "courier_actions"
    ]

# Пример: 'postgresql://username:password@localhost:5432/your_database'
engine = create_engine('postgresql://postgres:postgres@localhost:5440/externaldb')

start_time = time.time()

for table in tables_list:
    df = pd.read_csv(f"data/{table}_data.csv")
    df.to_sql(
    name=table, # имя таблицы
    con=engine,  # подключение
    if_exists="append", # если таблица уже существует, добавляем данные (чтобы не перезаписать типы столбцов)
    index=False # без индекса
    )

end_time = time.time()
total_time = round(end_time - start_time, 2)
print(f"Импорт таблиц завершён. Время вставки: {total_time} секунд(ы)")