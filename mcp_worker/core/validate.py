import os
import sys
import json
import pandas as pd

# Guarantee UTF-8 encoding for output, although this is the default in a Linux container
sys.stdout.reconfigure(encoding='utf-8')

INPUT_PATH = "/app/data/stage_buffer.csv"
RULES_PATH = "/app/store/business_rules.json"

def run_validation():
    # Verify check of input files existence
    if not os.path.exists(INPUT_PATH):
        print(f"Error: Input file {INPUT_PATH} is missing.", file=sys.stderr)
        sys.exit(1)
        
    if not os.path.exists(RULES_PATH):
        print(f"Error: Business rules file {RULES_PATH} is missing.", file=sys.stderr)
        sys.exit(1)

    try:
        df = pd.read_csv(INPUT_PATH)
        with open(RULES_PATH, "r", encoding="utf-8") as f:
            rules = json.load(f)
    except Exception as e:
        print(f"Error loading files: {e}", file=sys.stderr)
        sys.exit(1)

    validation_issues = []

    # Verify each column based on business rules (our RAG contract)
    for column, rule in rules.items():
        if column not in df.columns:
            validation_issues.append({
                "column": column,
                "error_type": "missing_column",
                "message": f"Column '{column}' is completely missing from the data structure."
            })
            continue

        # Check for empty values (NULL / NaN)
        if not rule.get("allow_null", True):
            # Check both standard nulls and empty strings after stripping
            null_mask = df[column].isnull() | (df[column].astype(str).str.strip() == "")
            null_indices = df[null_mask].index.tolist()
            
            if null_indices:
                validation_issues.append({
                    "column": column,
                    "error_type": "null_not_allowed",
                    "message": f"Empty values (NULL) detected in rows {null_indices}. Contract requires mandatory filling.",
                    "row_indices": null_indices
                })

    # If data contract violations are found
    if validation_issues:
        # Output structured JSON with a prefix marker to be intercepted by the worker
        print("DATA_ISSUES_JSON:" + json.dumps(validation_issues, ensure_ascii=False))
        sys.exit(1)

    # If everything is clean
    print("Validation successfully passed.")
    sys.exit(0)

if __name__ == "__main__":
    run_validation()