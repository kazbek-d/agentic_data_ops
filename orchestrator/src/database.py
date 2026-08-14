import os
from psycopg_pool import ConnectionPool
from langgraph.checkpoint.postgres import PostgresSaver


# Safely build connection string from environment variables (.env)
DB_USER = os.environ.get("POSTGRES_USER", "caedatallc_admin")
DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "SecretPassword2026")
DB_HOST = os.environ.get("POSTGRES_HOST", "postgres")
DB_PORT = os.environ.get("POSTGRES_PORT", "5432")
DB_NAME = os.environ.get("POSTGRES_DB", "dataops_checkpoints")

# Production connection string (URI) to our Postgres cluster
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


class DBManager:
    def __init__(self):
        print(f"💾 [DB] Initializing PostgresSaver connection pool at: {DB_HOST}:{DB_PORT}/{DB_NAME} (User: {DB_USER})", flush=True)
        
        # Connection parameter setup for psycopg
        # autocommit=True is critically important for correct schema creation and transactions within LangGraph
        connection_kwargs = {
            "autocommit": True,
            "prepare_threshold": 0,
        }

        # Create production connection pool
        self.pool = ConnectionPool(
            conninfo=DATABASE_URL,
            max_size=10,
            kwargs=connection_kwargs
        )
        
        
        # Pass the pool directly to PostgresSaver
        self.checkpointer = PostgresSaver(self.pool)
        
        # The .setup() method creates the necessary tables in the schema (checkpoints, 
        # checkpoint_blobs, checkpoint_writes, checkpoint_migrations) if they don't exist in the database yet.
        self.checkpointer.setup()
        print("✨ [DB] PostgresSaver table schema successfully verified/initialized in PostgreSQL!", flush=True)

    def get_checkpointer(self) -> PostgresSaver:
        """Returns the compiled PostgresSaver instance for LangGraph integration"""
        return self.checkpointer

# Export the database manager singleton
db_manager = DBManager()