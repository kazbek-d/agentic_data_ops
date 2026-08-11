import os
import sys
import pandas as pd

# Принудительно настраиваем вывод в UTF-8
sys.stdout.reconfigure(encoding='utf-8')

INPUT_PATH = "/app/data/stage_buffer.csv"

def run_transformation():
    if not os.path.exists(INPUT_PATH):
        print(f"Ошибка: Входной файл {INPUT_PATH} отсутствует.", file=sys.stderr)
        sys.exit(1)

    try:
        df = pd.read_csv(INPUT_PATH)
        
        # БИЗНЕС-ЛОГИКА: Рассчитываем бонус (10% от зарплаты).
        # Это хрупкое место: astype(float) упадет со свистом, если в данных будет 'SECRET'!
        df["bonus"] = df["salary"].astype(float) * 0.10
        
        # Сохраняем успешно обработанные данные обратно в буфер песочницы
        df.to_csv(INPUT_PATH, index=False)
        print("Трансформация успешно выполнена. Расчитан бонус 10% в колонке 'bonus'.")
        sys.exit(0)
        
    except ValueError as val_err:
        # Передаем подробный трейсбэк ошибки конвертации типов в stderr
        print(f"ValueError: could not convert string to float в колонке 'salary'. Нечисловые данные заблокировали расчет.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Критический рантайм-сбой при расчете трансформации: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    run_transformation()