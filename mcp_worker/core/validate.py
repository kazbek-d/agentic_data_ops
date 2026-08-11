import os
import sys
import json
import pandas as pd

# Гарантируем UTF-8 кодировку для вывода, хотя в Linux-контейнере это норма по умолчанию
sys.stdout.reconfigure(encoding='utf-8')

INPUT_PATH = "/app/data/stage_buffer.csv"
RULES_PATH = "/app/store/business_rules.json"

def run_validation():
    # Проверяем наличие входных файлов
    if not os.path.exists(INPUT_PATH):
        print(f"Ошибка: Входной файл {INPUT_PATH} отсутствует.", file=sys.stderr)
        sys.exit(1)
        
    if not os.path.exists(RULES_PATH):
        print(f"Ошибка: Файл бизнес-правил {RULES_PATH} отсутствует.", file=sys.stderr)
        sys.exit(1)

    try:
        df = pd.read_csv(INPUT_PATH)
        with open(RULES_PATH, "r", encoding="utf-8") as f:
            rules = json.load(f)
    except Exception as e:
        print(f"Ошибка при загрузке файлов: {e}", file=sys.stderr)
        sys.exit(1)

    validation_issues = []

    # Проверяем каждую колонку на основе бизнес-правил (наш RAG контракт)
    for column, rule in rules.items():
        if column not in df.columns:
            validation_issues.append({
                "column": column,
                "error_type": "missing_column",
                "message": f"Колонка '{column}' полностью отсутствует в структуре данных."
            })
            continue

        # Проверка на наличие пустых значений (NULL / NaN)
        if not rule.get("allow_null", True):
            # Проверяем как стандартные null, так и пустые строки после зачистки
            null_mask = df[column].isnull() | (df[column].astype(str).str.strip() == "")
            null_indices = df[null_mask].index.tolist()
            
            if null_indices:
                validation_issues.append({
                    "column": column,
                    "error_type": "null_not_allowed",
                    "message": f"Обнаружены пустые значения (NULL) в строках {null_indices}. Контракт требует обязательного заполнения.",
                    "row_indices": null_indices
                })

    # Если обнаружены нарушения контракта данных
    if validation_issues:
        # Выводим структурированный JSON с префиксом-маркером для перехвата воркером
        print("DATA_ISSUES_JSON:" + json.dumps(validation_issues, ensure_ascii=False))
        sys.exit(1)

    # Если всё чисто
    print("Валидация успешно пройдена.")
    sys.exit(0)

if __name__ == "__main__":
    run_validation()