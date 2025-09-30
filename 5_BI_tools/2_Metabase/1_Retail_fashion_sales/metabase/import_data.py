import pandas as pd
from sqlalchemy import create_engine
import time

# Пример: 'postgresql://username:password@localhost:5432/your_database'
engine = create_engine('postgresql://postgres:postgres@localhost:5440/externaldb')

start_time = time.time()

df = pd.read_csv(f"data/fashion_boutique_dataset.csv")
df.to_sql(
    name="fashion_boutique_sales", # имя таблицы
    con=engine,  # подключение
    if_exists="replace", # если таблица уже существует, заменяем данные
    index=False # без индекса
)

end_time = time.time()
total_time = round(end_time - start_time, 2)
print(f"Импорт таблиц завершён. Время вставки: {total_time} секунд(ы)")