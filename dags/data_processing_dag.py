# dags/data_processing_dag.py
import pandas as pd
import re
import os
from pendulum import datetime

from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models.dataset import Dataset

# --- 1. Константы ---
# Путь к файлу, который мы ждем (внутри Docker-контейнера)
# /opt/airflow/ - это "домашняя" папка проекта в Docker
FILE_TO_PROCESS = '/opt/airflow/include/airflow_data.csv'

# Путь, куда сохраним очищенные данные
CLEANED_FILE_PATH = '/opt/airflow/include/cleaned_data.csv'

# Объявляем "Набор данных", который будет "сигналом" для DAG 2
MONGO_READY_DATASET = Dataset("mongo_ready_data")


@dag(
    dag_id="data_processing_dag",  # Имя DAG в UI
    start_date=datetime(2025, 11, 1, tz="UTC"), # Дата начала
    schedule=None,        # Запускаться будет вручную
    catchup=False,        # Не запускать "пропущенные" запуски
    tags=["mongo_project"], # Теги для удобного поиска в UI
)
def data_processing_dag():
    """
    DAG, который ждет файл, обрабатывает его и готовит для загрузки в Mongo.
    """

    # --- 2. ЗАДАЧА: Сенсор ---
    # Ждет появления файла по пути FILE_TO_PROCESS
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=FILE_TO_PROCESS,
        mode="reschedule",    # Важно! "reschedule" освобождает воркер, пока ждет
        poke_interval=30,     # Проверять каждые 30 сек
        timeout=60 * 5,       # "Упасть" с ошибкой через 5 минут
    )

    # --- 3. ЗАДАЧА: Ветвление (Branching) ---
    # Проверяет, пустой файл или нет
    @task.branch
    def check_file_empty(file_path):
        """
        Если файл не пустой -> запускаем группу 'process_data_group'
        Если файл пустой -> запускаем задачу 'log_empty_file'
        """
        if os.path.getsize(file_path) > 0:
            # Имя задачи должно совпадать с task_id или group_id
            return "process_data_group"
        else:
            return "log_empty_file"

    # --- 4. ЗАДАЧА: Bash (для "пустого" файла) ---
    log_empty_file = BashOperator(
        task_id="log_empty_file",
        bash_command=f"echo 'Файл {FILE_TO_PROCESS} пуст, обработка не требуется.'"
    )

    # --- 5. ГРУППА ЗАДАЧ: Обработка (для "непустого" файла) ---
    @task_group(group_id="process_data_group")
    def process_data_group():
        """
        Эта группа задач выполняет всю обработку данных с Pandas.
        """

        @task
        def load_to_pandas(file_path):
            """Загружаем CSV в DataFrame."""
            df = pd.read_csv(file_path)
            return df

        @task
        def replace_nulls(df: pd.DataFrame):
            """Заменяем "null" на '-'."""
            df.replace("null", "-", inplace=True)
            return df

        @task
        def sort_by_date(df: pd.DataFrame):
            """Сортируем по дате."""
            df['created_date'] = pd.to_datetime(df['created_date'])
            df.sort_values(by='created_date', inplace=True)
            return df

        @task
        def clean_content(df: pd.DataFrame):
            """Очищаем 'content' от смайликов и спецсимволов."""
            def clean_text(text):
                # Оставляем только буквы (включая кириллицу), цифры, пробелы и базовую пунктуацию
                return re.sub(r'[^A-Za-zА-Яа-я0-9\s.,?!-]+', '', str(text))

            df['content'] = df['content'].apply(clean_text)
            return df

        @task(outlets=[MONGO_READY_DATASET]) # <-- Ключевой момент!
        def save_cleaned_data(df: pd.DataFrame):
            """
            Сохраняем DataFrame в CSV и "публикуем" Dataset.
            Это действие (outlets) запустит DAG 2.
            """
            df.to_csv(CLEANED_FILE_PATH, index=False)
            print(f"Очищенные данные сохранены в {CLEANED_FILE_PATH}")

        # Задаем порядок внутри TaskGroup
        df_loaded = load_to_pandas(FILE_TO_PROCESS)
        df_nulls_replaced = replace_nulls(df_loaded)
        df_sorted = sort_by_date(df_nulls_replaced)
        df_cleaned = clean_content(df_sorted)
        save_cleaned_data(df_cleaned)


    # --- 6. Задаем общий порядок выполнения ---

    branch_task = check_file_empty(file_path=FILE_TO_PROCESS)

    # Сенсор -> Ветвление -> (Либо Bash, ЛИБО TaskGroup)
    wait_for_file >> branch_task >> [log_empty_file, process_data_group()]


# Вызываем функцию, чтобы Airflow "увидел" DAG
data_processing_dag()