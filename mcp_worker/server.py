import os
import json
import subprocess
from typing import Any, Dict, Optional, List
from fastapi import FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field

# Импортируем официальный коннектор Snowflake
try:
    import snowflake.connector
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False

app = FastAPI(
    title="CAE Data LLC DataOps MCP Worker API (Snowflake-ready)",
    description="Вычислительный узел с поддержкой гибридного режима: локальные CSV или облачные таблицы Snowflake",
    version="2.0.0"
)

# =============================================================================
# НАСТРОЙКА ПУТЕЙ И КОНФИГУРАЦИИ Snowflake ИЗ .env
# =============================================================================
BASE_DIR = "/app"
DATA_DIR = os.path.join(BASE_DIR, "data")
CORE_DIR = os.path.join(BASE_DIR, "core")
STORE_DIR = os.path.join(BASE_DIR, "store")

BUFFER_PATH = os.path.join(DATA_DIR, "stage_buffer.csv")
RAW_DATA_PATH = os.path.join(DATA_DIR, "raw_salaries.csv")
RULES_PATH = os.path.join(STORE_DIR, "business_rules.json")

VALIDATE_SCRIPT = os.path.join(CORE_DIR, "validate.py")
TRANSFORM_SCRIPT = os.path.join(CORE_DIR, "transform.py")

# Параметры подключения к Snowflake
SF_USER = os.environ.get("SNOWFLAKE_USER")
SF_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD")
SF_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT")
SF_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE")
SF_DATABASE = os.environ.get("SNOWFLAKE_DATABASE")
SF_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")

# Проверяем, настроен ли живой режим Snowflake
USE_SNOWFLAKE = all([SF_USER, SF_PASSWORD, SF_ACCOUNT, SF_DATABASE]) and SNOWFLAKE_AVAILABLE

# =============================================================================
# PYDANTIC КОНТРАКТЫ ДЛЯ API
# =============================================================================
class PatchRequest(BaseModel):
    column: str = Field(..., description="Имя колонки для применения патча")
    action: str = Field(..., description="Тип действия: fill_na или replace")
    value: Any = Field(..., description="Значение для подстановки")

class ExecutionResponse(BaseModel):
    success: bool
    message: str
    stderr: Optional[str] = None
    data: Optional[Any] = None

# =============================================================================
# ВСПОМОГАТЕЛЬНЫЙ МЕНЕДЖЕР ОБЛАЧНОГО СОСТОЯНИЯ SNOWFLAKE
# =============================================================================
class SnowflakeSandbox:
    def __init__(self):
        self.conn = None

    def get_connection(self):
        """Создает и возвращает сессию подключения к Snowflake"""
        return snowflake.connector.connect(
            user=SF_USER,
            password=SF_PASSWORD,
            account=SF_ACCOUNT,
            warehouse=SF_WAREHOUSE,
            database=SF_DATABASE,
            schema=SF_SCHEMA
        )

    def initialize_stage_tables(self):
        """
        Имитирует копирование грязных данных из продакшн-таблицы 
        во временную стейдж-таблицу (sandbox buffer) внутри Snowflake.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            print("❄️ [SNOWFLAKE] Создание демонстрационных таблиц...", flush=True)
            # 1. Создаем сырую 'грязную' таблицу, если её нет (эмуляция прод-данных)
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {SF_DATABASE}.{SF_SCHEMA}.salaries_prod (
                    employee_id INT,
                    name VARCHAR,
                    salary VARCHAR,
                    department_code VARCHAR,
                    bonus FLOAT
                )
            """)
            
            # Наполняем её базовыми аномалиями (если пустая)
            cursor.execute(f"SELECT COUNT(*) FROM {SF_DATABASE}.{SF_SCHEMA}.salaries_prod")
            if cursor.fetchone()[0] == 0:
                cursor.execute(f"""
                    INSERT INTO {SF_DATABASE}.{SF_SCHEMA}.salaries_prod (employee_id, name, salary, department_code)
                    VALUES 
                    (101, 'John Doe', '5000.00', 'HR'),
                    (102, 'Jane Smith', '6200.50', 'IT'),
                    (103, 'Bob Johnson', 'SECRET', 'FIN'),
                    (104, 'Alice Brown', '4800.00', NULL)
                """)
            
            # 2. Создаем чистую копию-буфер (наш Sandbox Stage)
            cursor.execute(f"CREATE OR REPLACE TABLE {SF_DATABASE}.{SF_SCHEMA}.salaries_stage CLONE {SF_DATABASE}.{SF_SCHEMA}.salaries_prod")
            print("✨ [SNOWFLAKE] Буфер salaries_stage успешно развернут!", flush=True)
        finally:
            cursor.close()
            conn.close()

    def get_data_snapshot(self) -> List[Dict[str, Any]]:
        """Извлекает текущее состояние буфера для отображения в дешборде"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT employee_id, name, salary, department_code, bonus FROM {SF_DATABASE}.{SF_SCHEMA}.salaries_stage ORDER BY employee_id")
            columns = [col[0].lower() for col in cursor.description]
            results = []
            for row in cursor.fetchall():
                # Преобразуем None в строку 'null' для полной совместимости с фронтендом
                row_dict = dict(zip(columns, row))
                if row_dict["department_code"] is None:
                    row_dict["department_code"] = "null"
                results.append(row_dict)
            return results
        finally:
            cursor.close()
            conn.close()

sf_manager = SnowflakeSandbox() if USE_SNOWFLAKE else None

# =============================================================================
# ИНИЦИАЛИЗАЦИЯ ПЕСОЧНИЦЫ (SANDBOX) ПРИ СТАРТЕ
# =============================================================================
@app.on_event("startup")
def initialize_sandbox():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(STORE_DIR, exist_ok=True)
    os.makedirs(CORE_DIR, exist_ok=True)
    
    if USE_SNOWFLAKE:
        print("❄️ [WORKER MODE] Запуск в режиме интеграции с облаком Snowflake!", flush=True)
        try:
            sf_manager.initialize_stage_tables()
        except Exception as e:
            print(f"🚨 [SNOWFLAKE] Критический сбой инициализации в облаке: {e}", flush=True)
    else:
        print("💻 [WORKER MODE] Запуск в локальном режиме (Local CSV Sandbox)", flush=True)
        # Симулируем генерацию исходных грязных данных на диске
        if not os.path.exists(RAW_DATA_PATH):
            import pandas as pd
            mock_data = {
                "employee_id": [101, 102, 103, 104],
                "name": ["John Doe", "Jane Smith", "Bob Johnson", "Alice Brown"],
                "salary": ["5000.00", "6200.50", "SECRET", "4800.00"],
                "department_code": ["HR", "IT", "FIN", None]
            }
            pd.DataFrame(mock_data).to_csv(RAW_DATA_PATH, index=False)

        # Копируем буфер
        import shutil
        shutil.copyfile(RAW_DATA_PATH, BUFFER_PATH)

# =============================================================================
# API ЭНДПОИНТЫ ВОРКЕРА
# =============================================================================
@app.post("/api/v1/initialize", response_model=ExecutionResponse)
def reset_sandbox():
    try:
        if USE_SNOWFLAKE:
            sf_manager.initialize_stage_tables()
            return ExecutionResponse(success=True, message="Облачная таблица salaries_stage успешно пересоздана в Snowflake.")
        else:
            import shutil
            shutil.copyfile(RAW_DATA_PATH, BUFFER_PATH)
            return ExecutionResponse(success=True, message="Локальный буфер CSV успешно сброшен.")
    except Exception as e:
        return ExecutionResponse(success=False, message=f"Ошибка сброса песочницы: {e}")

@app.post("/api/v1/validate", response_model=ExecutionResponse)
def run_validation():
    """Синхронный запуск валидации (в облаке Snowflake или на диске)"""
    if USE_SNOWFLAKE:
        # Валидация средствами Snowflake SQL-запроса!
        conn = sf_manager.get_connection()
        cursor = conn.cursor()
        try:
            # Читаем правила из JSON
            with open(RULES_PATH, "r", encoding="utf-8") as f:
                rules = json.load(f)
            
            validation_issues = []
            
            # Проверяем некритичное поле department_code
            if not rules.get("department_code", {}).get("allow_null", True):
                cursor.execute(f"SELECT employee_id FROM {SF_DATABASE}.{SF_SCHEMA}.salaries_stage WHERE department_code IS NULL OR TRIM(department_code) = ''")
                null_rows = [row[0] for row in cursor.fetchall()]
                if null_rows:
                    validation_issues.append({
                        "column": "department_code",
                        "error_type": "null_not_allowed",
                        "message": f"Обнаружены пустые значения (NULL) в строках (ID сотрудников): {null_rows}.",
                        "row_indices": null_rows
                    })
                    
            if validation_issues:
                return ExecutionResponse(
                    success=False,
                    message="Валидатор зафиксировал несоответствие бизнес-правилам в Snowflake.",
                    data=validation_issues
                )
            return ExecutionResponse(success=True, message="Валидация в Snowflake пройдена.")
        except Exception as e:
            return ExecutionResponse(success=False, message=f"Ошибка SQL-валидации: {e}")
        finally:
            cursor.close()
            conn.close()
    else:
        # Локальный режим subprocess
        if not os.path.exists(VALIDATE_SCRIPT):
            return ExecutionResponse(success=False, message="Скрипт валидации не найден.", stderr="FileNotFoundError")
        result = subprocess.run(["python", VALIDATE_SCRIPT], capture_output=True, text=True, encoding="utf-8")
        if result.returncode == 0:
            return ExecutionResponse(success=True, message="Локальная валидация пройдена.")
        if "DATA_ISSUES_JSON:" in result.stdout:
            issues = json.loads(result.stdout.split("DATA_ISSUES_JSON:")[1].strip())
            return ExecutionResponse(success=False, message="Локальный валидатор нашел сбои.", data=issues)
        return ExecutionResponse(success=False, message="Системный сбой локальной валидации.", stderr=result.stderr)

@app.post("/api/v1/transform", response_model=ExecutionResponse)
def run_transformation():
    """Синхронный запуск трансформации (облачный SQL-расчет бонусов или Pandas)"""
    if USE_SNOWFLAKE:
        conn = sf_manager.get_connection()
        cursor = conn.cursor()
        try:
            # Пытаемся рассчитать бонус в Snowflake: UPDATE salaries_stage SET bonus = CAST(salary AS FLOAT) * 0.10
            # Если в salary лежит строка 'SECRET', Snowflake упадет с ошибкой преобразования типов!
            cursor.execute(f"""
                UPDATE {SF_DATABASE}.{SF_SCHEMA}.salaries_stage
                SET bonus = TO_DOUBLE(salary) * 0.10
            """)
            return ExecutionResponse(success=True, message="Облачная трансформация успешно завершена. Бонусы рассчитаны.")
        except snowflake.connector.errors.ProgrammingError as e:
            # Перехватываем ошибку конвертации типа и возвращаем ее в оркестратор как сбой трансформации
            return ExecutionResponse(
                success=False, 
                message="Ошибка трансформации в Snowflake: тип данных не может быть приведен к числу.", 
                stderr=str(e)
            )
        finally:
            cursor.close()
            conn.close()
    else:
        # Локальный режим Pandas
        if not os.path.exists(TRANSFORM_SCRIPT):
            return ExecutionResponse(success=False, message="Скрипт трансформации не найден.")
        result = subprocess.run(["python", TRANSFORM_SCRIPT], capture_output=True, text=True, encoding="utf-8")
        if result.returncode == 0:
            return ExecutionResponse(success=True, message="Локальная трансформация успешно выполнена.")
        return ExecutionResponse(success=False, message="Локальный трансформатор упал.", stderr=result.stderr)

@app.post("/api/v1/patch", response_model=ExecutionResponse)
def apply_patch(req: PatchRequest):
    """Применение физического патча (через SQL UPDATE в Snowflake или Pandas)"""
    if USE_SNOWFLAKE:
        conn = sf_manager.get_connection()
        cursor = conn.cursor()
        try:
            if req.action == "fill_na":
                # Заполняем NULL значения
                cursor.execute(f"""
                    UPDATE {SF_DATABASE}.{SF_SCHEMA}.salaries_stage
                    SET {req.column} = %s
                    WHERE {req.column} IS NULL OR TRIM({req.column}) = ''
                """, (req.value,))
                msg = f"Облачный патч [fill_na] успешно выполнен в Snowflake для колонки '{req.column}'."
                
            elif req.action == "replace":
                # Заменяем нечисловые аномалии на дефолт.
                # Пишем SQL-запрос, который пытается распарсить строку, и если это не число — меняет на req.value
                cursor.execute(f"""
                    UPDATE {SF_DATABASE}.{SF_SCHEMA}.salaries_stage
                    SET {req.column} = %s
                    WHERE TRY_TO_DOUBLE({req.column}) IS NULL AND {req.column} IS NOT NULL
                """, (req.value,))
                msg = f"Облачный патч [replace] успешно зачистил нечисловые аномалии в Snowflake для колонки '{req.column}'."
            else:
                return ExecutionResponse(success=False, message="Неизвестный тип патча.")
                
            return ExecutionResponse(success=True, message=msg)
        except Exception as e:
            return ExecutionResponse(success=False, message=f"Ошибка наката SQL-патча: {e}")
        finally:
            cursor.close()
            conn.close()
    else:
        # Локальный режим работы с Pandas файлом
        if not os.path.exists(BUFFER_PATH):
            return ExecutionResponse(success=False, message="Локальный буфер отсутствует.")
        try:
            import pandas as pd
            df = pd.read_csv(BUFFER_PATH)
            if req.column not in df.columns:
                return ExecutionResponse(success=False, message=f"Колонка '{req.column}' не найдена.")
                
            if req.action == "fill_na":
                df[req.column] = df[req.column].fillna(req.value)
                msg = f"Локальный патч [fill_na] применен для '{req.column}'."
            elif req.action == "replace":
                converted = pd.to_numeric(df[req.column], errors='coerce')
                df[req.column] = converted.fillna(float(req.value))
                msg = f"Локальный патч [replace] очистил аномалии в '{req.column}'."
                
            df.to_csv(BUFFER_PATH, index=False)
            return ExecutionResponse(success=True, message=msg)
        except Exception as e:
            return ExecutionResponse(success=False, message=f"Ошибка наката локального патча: {e}")

@app.get("/api/v1/business_rules", response_model=ExecutionResponse)
def get_business_rules():
    if not os.path.exists(RULES_PATH):
        return ExecutionResponse(success=False, message="Правила не найдены.")
    with open(RULES_PATH, "r", encoding="utf-8") as f:
        rules = json.load(f)
    return ExecutionResponse(success=True, message="Правила прочитаны.", data=rules)

@app.get("/api/v1/code", response_model=ExecutionResponse)
def get_script_code(script_type: str = Query(..., pattern="^(validate|transform)$")):
    target_path = VALIDATE_SCRIPT if script_type == "validate" else TRANSFORM_SCRIPT
    if not os.path.exists(target_path):
        return ExecutionResponse(success=False, message="Код скрипта не найден.")
    with open(target_path, "r", encoding="utf-8") as f:
        code = f.read()
    return ExecutionResponse(success=True, message="Код прочитан.", data=code)

# =============================================================================
# ТОЧКА ВХОДА
# =============================================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=5011, reload=False)