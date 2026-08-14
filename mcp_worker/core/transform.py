import os
import sys
import pandas as pd

# Force stdout to UTF-8
sys.stdout.reconfigure(encoding='utf-8')

INPUT_PATH = "/app/data/stage_buffer.csv"

def run_transformation():
    if not os.path.exists(INPUT_PATH):
        print(f"Error: Input file {INPUT_PATH} is missing.", file=sys.stderr)
        sys.exit(1)

    try:
        df = pd.read_csv(INPUT_PATH)
        
        # BUSINESS LOGIC: Calculate bonus (10% of salary).
        # This is a fragile place: astype(float) will crash if there is 'SECRET' in the data!
        df["bonus"] = df["salary"].astype(float) * 0.10
        
        # Save successfully processed data back to sandbox buffer
        df.to_csv(INPUT_PATH, index=False)
        print("Transformation successfully completed. Calculated 10% bonus in the 'bonus' column.")
        sys.exit(0)
        
    except ValueError as val_err:
        # Pass detailed traceback of type conversion error to stderr
        print(f"ValueError: could not convert string to float in column 'salary'. Non-numeric data blocked the calculation.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Critical runtime failure during transformation calculation: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    run_transformation()