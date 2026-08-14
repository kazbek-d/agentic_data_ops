import os
import json
import subprocess
from typing import Any, Dict, Optional, List
from fastapi import FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field

# Import the official Snowflake connector
try:
    import snowflake.connector
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False

app = FastAPI(
    title="CAE Data LLC DataOps MCP Worker API (Snowflake-ready)",
    description="Compute node with hybrid mode support: local CSV or cloud Snowflake tables",
    version="2.0.0"
)

# =============================================================================
# CONFIGURING PATHS AND SNOWFLAKE SETUP FROM .env
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

# Snowflake connection parameters
SF_USER = os.environ.get("SNOWFLAKE_USER")
SF_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD")
SF_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT")
SF_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE")
SF_DATABASE = os.environ.get("SNOWFLAKE_DATABASE")
SF_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")

# Check if Snowflake live mode is configured
USE_SNOWFLAKE = all([SF_USER, SF_PASSWORD, SF_ACCOUNT, SF_DATABASE]) and SNOWFLAKE_AVAILABLE

# =============================================================================
# PYDANTIC API CONTRACTS
# =============================================================================
class PatchRequest(BaseModel):
    column: str = Field(..., description="Column name to apply the patch to")
    action: str = Field(..., description="Action type: fill_na or replace")
    value: Any = Field(..., description="Value to substitute")

class ExecutionResponse(BaseModel):
    success: bool
    message: str
    stderr: Optional[str] = None
    data: Optional[Any] = None

# =============================================================================
# SNOWFLAKE CLOUD STATE HELPER MANAGER
# =============================================================================
class SnowflakeSandbox:
    def __init__(self):
        self.conn = None

    def get_connection(self):
        """Creates and returns Snowflake connection session"""
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
        Simulates copying dirty data from production table 
        to temporary stage table (sandbox buffer) inside Snowflake.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            print("❄️ [SNOWFLAKE] Creating demo tables...", flush=True)
            # 1. Create raw 'dirty' table if it doesn't exist (prod data emulation)
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {SF_DATABASE}.{SF_SCHEMA}.salaries_prod (
                    employee_id INT,
                    name VARCHAR,
                    salary VARCHAR,
                    department_code VARCHAR,
                    bonus FLOAT
                )
            """)
            
            # Populate it with basic anomalies (if empty)
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
            
            # 2. Create clean copy-buffer (our Sandbox Stage)
            cursor.execute(f"CREATE OR REPLACE TABLE {SF_DATABASE}.{SF_SCHEMA}.salaries_stage CLONE {SF_DATABASE}.{SF_SCHEMA}.salaries_prod")
            print("✨ [SNOWFLAKE] salaries_stage buffer successfully deployed!", flush=True)
        finally:
            cursor.close()
            conn.close()

    def get_data_snapshot(self) -> List[Dict[str, Any]]:
        """Extracts current buffer state to display in dashboard"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT employee_id, name, salary, department_code, bonus FROM {SF_DATABASE}.{SF_SCHEMA}.salaries_stage ORDER BY employee_id")
            columns = [col[0].lower() for col in cursor.description]
            results = []
            for row in cursor.fetchall():
                # Convert None to string 'null' for full frontend compatibility
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
# SANDBOX INITIALIZATION ON STARTUP
# =============================================================================
@app.on_event("startup")
def initialize_sandbox():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(STORE_DIR, exist_ok=True)
    os.makedirs(CORE_DIR, exist_ok=True)
    
    if USE_SNOWFLAKE:
        print("❄️ [WORKER MODE] Running in Snowflake cloud integration mode!", flush=True)
        try:
            sf_manager.initialize_stage_tables()
        except Exception as e:
            print(f"🚨 [SNOWFLAKE] Critical cloud initialization failure: {e}", flush=True)
    else:
        print("💻 [WORKER MODE] Running in local mode (Local CSV Sandbox)", flush=True)
        # Simulate raw dirty data generation on disk
        if not os.path.exists(RAW_DATA_PATH):
            import pandas as pd
            mock_data = {
                "employee_id": [101, 102, 103, 104],
                "name": ["John Doe", "Jane Smith", "Bob Johnson", "Alice Brown"],
                "salary": ["5000.00", "6200.50", "SECRET", "4800.00"],
                "department_code": ["HR", "IT", "FIN", None]
            }
            pd.DataFrame(mock_data).to_csv(RAW_DATA_PATH, index=False)

        # Copy the buffer
        import shutil
        shutil.copyfile(RAW_DATA_PATH, BUFFER_PATH)

# =============================================================================
# WORKER API ENDPOINTS
# =============================================================================
@app.post("/api/v1/initialize", response_model=ExecutionResponse)
def reset_sandbox():
    try:
        if USE_SNOWFLAKE:
            sf_manager.initialize_stage_tables()
            return ExecutionResponse(success=True, message="Cloud table salaries_stage successfully recreated in Snowflake.")
        else:
            import shutil
            shutil.copyfile(RAW_DATA_PATH, BUFFER_PATH)
            return ExecutionResponse(success=True, message="Local CSV buffer successfully reset.")
    except Exception as e:
        return ExecutionResponse(success=False, message=f"Sandbox reset error: {e}")

@app.post("/api/v1/validate", response_model=ExecutionResponse)
def run_validation():
    """Synchronous validation run (in Snowflake cloud or on disk)"""
    if USE_SNOWFLAKE:
        # Validation using Snowflake SQL query!
        conn = sf_manager.get_connection()
        cursor = conn.cursor()
        try:
            # Read rules from JSON
            with open(RULES_PATH, "r", encoding="utf-8") as f:
                rules = json.load(f)
            
            validation_issues = []
            
            # Check non-critical department_code field
            if not rules.get("department_code", {}).get("allow_null", True):
                cursor.execute(f"SELECT employee_id FROM {SF_DATABASE}.{SF_SCHEMA}.salaries_stage WHERE department_code IS NULL OR TRIM(department_code) = ''")
                null_rows = [row[0] for row in cursor.fetchall()]
                if null_rows:
                    validation_issues.append({
                        "column": "department_code",
                        "error_type": "null_not_allowed",
                        "message": f"Empty values (NULL) detected in rows (Employee IDs): {null_rows}.",
                        "row_indices": null_rows
                    })
                    
            if validation_issues:
                return ExecutionResponse(
                    success=False,
                    message="Validator detected business rules violation in Snowflake.",
                    data=validation_issues
                )
            return ExecutionResponse(success=True, message="Validation in Snowflake passed.")
        except Exception as e:
            return ExecutionResponse(success=False, message=f"SQL validation error: {e}")
        finally:
            cursor.close()
            conn.close()
    else:
        # Local subprocess mode
        if not os.path.exists(VALIDATE_SCRIPT):
            return ExecutionResponse(success=False, message="Validation script not found.", stderr="FileNotFoundError")
        result = subprocess.run(["python", VALIDATE_SCRIPT], capture_output=True, text=True, encoding="utf-8")
        if result.returncode == 0:
            return ExecutionResponse(success=True, message="Local validation passed.")
        if "DATA_ISSUES_JSON:" in result.stdout:
            issues = json.loads(result.stdout.split("DATA_ISSUES_JSON:")[1].strip())
            return ExecutionResponse(success=False, message="Local validator found issues.", data=issues)
        return ExecutionResponse(success=False, message="System failure of local validation.", stderr=result.stderr)

@app.post("/api/v1/transform", response_model=ExecutionResponse)
def run_transformation():
    """Synchronous transformation run (cloud SQL bonus calculation or Pandas)"""
    if USE_SNOWFLAKE:
        conn = sf_manager.get_connection()
        cursor = conn.cursor()
        try:
            # Try to calculate bonus in Snowflake: UPDATE salaries_stage SET bonus = CAST(salary AS FLOAT) * 0.10
            # If salary contains the string 'SECRET', Snowflake will fail with a type conversion error!
            cursor.execute(f"""
                UPDATE {SF_DATABASE}.{SF_SCHEMA}.salaries_stage
                SET bonus = TO_DOUBLE(salary) * 0.10
            """)
            return ExecutionResponse(success=True, message="Cloud transformation successfully completed. Bonuses calculated.")
        except snowflake.connector.errors.ProgrammingError as e:
            # Intercept type conversion error and return it to the orchestrator as transformation failure
            return ExecutionResponse(
                success=False, 
                message="Transformation error in Snowflake: data type cannot be cast to number.", 
                stderr=str(e)
            )
        finally:
            cursor.close()
            conn.close()
    else:
        # Local Pandas mode
        if not os.path.exists(TRANSFORM_SCRIPT):
            return ExecutionResponse(success=False, message="Transformation script not found.")
        result = subprocess.run(["python", TRANSFORM_SCRIPT], capture_output=True, text=True, encoding="utf-8")
        if result.returncode == 0:
            return ExecutionResponse(success=True, message="Local transformation successfully completed.")
        return ExecutionResponse(success=False, message="Local transformer failed.", stderr=result.stderr)

@app.post("/api/v1/patch", response_model=ExecutionResponse)
def apply_patch(req: PatchRequest):
    """Applying a physical patch (via SQL UPDATE in Snowflake or Pandas)"""
    if USE_SNOWFLAKE:
        conn = sf_manager.get_connection()
        cursor = conn.cursor()
        try:
            if req.action == "fill_na":
                # Fill NULL values
                cursor.execute(f"""
                    UPDATE {SF_DATABASE}.{SF_SCHEMA}.salaries_stage
                    SET {req.column} = %s
                    WHERE {req.column} IS NULL OR TRIM({req.column}) = ''
                """, (req.value,))
                msg = f"Cloud patch [fill_na] successfully executed in Snowflake for column '{req.column}'."
                
            elif req.action == "replace":
                # Replace non-numeric anomalies with default value.
                # Write an SQL query that tries to parse the string, and if it's not a number, changes it to req.value
                cursor.execute(f"""
                    UPDATE {SF_DATABASE}.{SF_SCHEMA}.salaries_stage
                    SET {req.column} = %s
                    WHERE TRY_TO_DOUBLE({req.column}) IS NULL AND {req.column} IS NOT NULL
                """, (req.value,))
                msg = f"Cloud patch [replace] successfully cleaned non-numeric anomalies in Snowflake for column '{req.column}'."
            else:
                return ExecutionResponse(success=False, message="Unknown patch type.")
                
            return ExecutionResponse(success=True, message=msg)
        except Exception as e:
            return ExecutionResponse(success=False, message=f"SQL patch application error: {e}")
        finally:
            cursor.close()
            conn.close()
    else:
        # Local Pandas file mode
        if not os.path.exists(BUFFER_PATH):
            return ExecutionResponse(success=False, message="Local buffer is missing.")
        try:
            import pandas as pd
            df = pd.read_csv(BUFFER_PATH)
            if req.column not in df.columns:
                return ExecutionResponse(success=False, message=f"Column '{req.column}' not found.")
                
            if req.action == "fill_na":
                df[req.column] = df[req.column].fillna(req.value)
                msg = f"Local patch [fill_na] applied for '{req.column}'."
            elif req.action == "replace":
                converted = pd.to_numeric(df[req.column], errors='coerce')
                df[req.column] = converted.fillna(float(req.value))
                msg = f"Local patch [replace] cleaned anomalies in '{req.column}'."
                
            df.to_csv(BUFFER_PATH, index=False)
            return ExecutionResponse(success=True, message=msg)
        except Exception as e:
            return ExecutionResponse(success=False, message=f"Local patch application error: {e}")

@app.get("/api/v1/business_rules", response_model=ExecutionResponse)
def get_business_rules():
    if not os.path.exists(RULES_PATH):
        return ExecutionResponse(success=False, message="Rules not found.")
    with open(RULES_PATH, "r", encoding="utf-8") as f:
        rules = json.load(f)
    return ExecutionResponse(success=True, message="Rules read successfully.", data=rules)

@app.get("/api/v1/code", response_model=ExecutionResponse)
def get_script_code(script_type: str = Query(..., pattern="^(validate|transform)$")):
    target_path = VALIDATE_SCRIPT if script_type == "validate" else TRANSFORM_SCRIPT
    if not os.path.exists(target_path):
        return ExecutionResponse(success=False, message="Script code not found.")
    with open(target_path, "r", encoding="utf-8") as f:
        code = f.read()
    return ExecutionResponse(success=True, message="Code read successfully.", data=code)

# =============================================================================
# ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=5011, reload=False)