# file: 03_create_csv_classifier.py
# Custom classifier ensures crawler correctly handles CSV headers and delimiters
# Prevents Pitfall 3 and 8 described above

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-2"


def create_csv_classifier():
    """
    Custom CSV classifier with explicit settings
    
    WHY CUSTOM vs BUILT-IN:
    → Built-in CSV classifier uses heuristics for delimiter/header detection
    → Sometimes gets it wrong (especially with quoted fields containing commas)
    → Custom classifier: you specify EXACTLY how your CSVs work
    → Custom classifiers are checked BEFORE built-in (higher priority)
    
    SETTINGS:
    → Delimiter: "," (our CSVs are comma-separated)
    → Quote symbol: '"' (standard double-quote)
    → Contains header: True (first row is column names)
    → Disable value: empty string not treated as null
    → Allow single column: False (safety — single column usually means wrong delimiter)
    """
    glue_client = boto3.client("glue", region_name=REGION)

    classifier_name = "quickcart-csv-with-header"

    try:
        glue_client.create_classifier(
            CsvClassifier={
                "Name": classifier_name,
                "Delimiter": ",",
                "QuoteSymbol": '"',
                "ContainsHeader": "PRESENT",     # PRESENT / ABSENT / UNKNOWN
                "DisableValueTrimming": False,    # Trim whitespace from values
                "AllowSingleColumn": False        # Safety: reject single-column detection
            }
        )
        print(f"✅ CSV classifier created: {classifier_name}")

    except ClientError as e:
        if "AlreadyExistsException" in str(e):
            # Update existing
            glue_client.update_classifier(
                CsvClassifier={
                    "Name": classifier_name,
                    "Delimiter": ",",
                    "QuoteSymbol": '"',
                    "ContainsHeader": "PRESENT",
                    "DisableValueTrimming": False,
                    "AllowSingleColumn": False
                }
            )
            print(f"ℹ️  CSV classifier updated: {classifier_name}")
        else:
            raise

    # Verify
    response = glue_client.get_classifier(Name=classifier_name)
    clf = response["Classifier"]["CsvClassifier"]
    print(f"\n📋 Classifier details:")
    print(f"   Name:        {clf['Name']}")
    print(f"   Delimiter:   '{clf['Delimiter']}'")
    print(f"   Quote:       '{clf['QuoteSymbol']}'")
    print(f"   Has Header:  {clf['ContainsHeader']}")

    return classifier_name


if __name__ == "__main__":
    create_csv_classifier()