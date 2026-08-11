import os
from psycopg_pool import ConnectionPool
from langgraph.checkpoint.postgres import PostgresSaver


# Безопасно формируем строку подключения из переменных окружения (.env)
DB_USER = os.environ.get("POSTGRES_USER", "caedatallc_admin")
DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "SecretPassword2026")
DB_HOST = os.environ.get("POSTGRES_HOST", "postgres")
DB_PORT = os.environ.get("POSTGRES_PORT", "5432")
DB_NAME = os.environ.get("POSTGRES_DB", "dataops_checkpoints")

# Промышленная строка подключения (URI) к нашему Postgres-кластеру
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


class DBManager:
    def __init__(self):
        print(f"💾 [DB] Инициализация пула соединений PostgresSaver по адресу: {DB_HOST}:{DB_PORT}/{DB_NAME} (User: {DB_USER})", flush=True)
        
        # Настройка параметров подключения для psycopg
        # autocommit=True критически важен для корректного создания схем и транзакций внутри LangGraph
        connection_kwargs = {
            "autocommit": True,
            "prepare_threshold": 0,
        }

        # Создаем промышленный пул соединений
        self.pool = ConnectionPool(
            conninfo=DATABASE_URL,
            max_size=10,
            kwargs=connection_kwargs
        )
        
        
        # Передаем пул напрямую в PostgresSaver
        self.checkpointer = PostgresSaver(self.pool)
        
        # Метод .setup() создает необходимые таблицы в схеме (checkpoints, 
        # checkpoint_blobs, checkpoint_writes, checkpoint_migrations), если их еще нет в базе.
        self.checkpointer.setup()
        print("✨ [DB] Схема таблиц PostgresSaver успешно проверена/инициализирована в PostgreSQL!", flush=True)

    def get_checkpointer(self) -> PostgresSaver:
        """Возвращает скомпилированный инстанс PostgresSaver для интеграции в LangGraph"""
        return self.checkpointer

# Экспортируем синглтон менеджера базы данных
db_manager = DBManager()