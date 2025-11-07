# dags/mongo_load_dag.py
import pandas as pd
from pendulum import datetime

from airflow.decorators import dag, task
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.models.dataset import Dataset

# --- 1. Константы ---

# Файл, который мы читаем (тот же, что мы создали в DAG 1)
CLEANED_FILE_PATH = '/usr/local/airflow/include/cleaned_data.csv'

# Dataset, на который мы "подписаны" (тот же, что объявили в DAG 1)
MONGO_READY_DATASET = Dataset("mongo_ready_data")

# ID нашего Mongo-подключения (тот, что создали в UI)
MONGO_CONN_ID = "mongo_default"


@dag(
    dag_id="mongo_load_dag",
    start_date=datetime(2025, 11, 1, tz="UTC"),

    # === ВАЖНО: ЗАПУСК ПО DATASET ===
    # Этот DAG запустится ТОЛЬКО тогда,
    # когда другой DAG "опубликует" MONGO_READY_DATASET
    schedule=[MONGO_READY_DATASET],

    catchup=False,
    tags=["mongo_project"],
)
def mongo_load_dag():
    """
    Этот DAG "слушает" Dataset от data_processing_dag
    и загружает готовые данные в MongoDB.
    """

    @task
    def load_to_mongo(mongo_conn_id: str, db_name: str, coll_name: str):
        """
        Подключается к Mongo и загружает данные из CSV.
        """
        print(f"Чтение файла: {CLEANED_FILE_PATH}")
        # 1. Читаем обработанный CSV
        df = pd.read_csv(CLEANED_FILE_PATH)

        # 2. Конвертируем DataFrame в список словарей (формат JSON)
        records = df.to_dict('records')
        print(f"Конвертировано {len(records)} записей для MongoDB.")

        # 3. Подключаемся к Mongo с помощью "хука"
        hook = MongoHook(conn_id=mongo_conn_id)
        client = hook.get_conn()
        db = client[db_name] # Выбираем Базу Данных
        collection = db[coll_name] # Выбираем Коллекцию

        print(f"Подключено к MongoDB, база: {db_name}, коллекция: {coll_name}")

        # 4. Очищаем коллекцию (чтобы не было дублей при перезапусках)
        collection.delete_many({})
        print("Старая коллекция очищена.")

        # 5. Вставляем данные
        collection.insert_many(records)
        print(f"УСПЕХ: Загружено {len(records)} записей в MongoDB.")

        client.close()

    # Вызываем нашу задачу
    load_to_mongo(
        mongo_conn_id=MONGO_CONN_ID,
        db_name="airflow_db",    # Можешь назвать БД как хочешь
        coll_name="comments"     # Можешь назвать коллекцию как хочешь
    )

# Вызываем функцию, чтобы Airflow "увидел" DAG
mongo_load_dag()