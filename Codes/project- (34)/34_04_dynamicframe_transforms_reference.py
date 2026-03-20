# file: 34_04_dynamicframe_transforms_reference.py
# Purpose: Show ALL DynamicFrame-native transforms for reference
# These are alternatives to PySpark when you need Glue-specific features
# NOT a standalone job — reference snippets for use inside Glue scripts

from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame


def dynamicframe_transform_catalog(glueContext, dyf):
    """
    COMPLETE CATALOG of DynamicFrame transforms
    Each shown with example usage
    """

    # ═══════════════════════════════════════════════════════════
    # TRANSFORM 1: ApplyMapping
    # ═══════════════════════════════════════════════════════════
    # Rename columns + cast types in ONE declaration
    # Most common DynamicFrame transform
    mapped = ApplyMapping.apply(
        frame=dyf,
        mappings=[
            # (source_col, source_type, target_col, target_type)
            ("order_id", "string", "order_id", "string"),
            ("customer_id", "string", "cust_id", "string"),       # RENAMED
            ("total_amount", "string", "total_amount", "double"),  # CAST
            ("order_date", "string", "order_date", "date"),        # CAST
            ("status", "string", "order_status", "string")         # RENAMED
            # Columns NOT listed are DROPPED
        ],
        transformation_ctx="apply_mapping"
    )

    # ═══════════════════════════════════════════════════════════
    # TRANSFORM 2: Filter
    # ═══════════════════════════════════════════════════════════
    # Remove rows based on a predicate function
    # Function receives a DynamicRecord, returns True to keep
    filtered = Filter.apply(
        frame=dyf,
        f=lambda row: (
            row["customer_id"] is not None
            and row["total_amount"] is not None
            and float(row["total_amount"]) > 0
        ),
        transformation_ctx="filter_valid"
    )

    # ═══════════════════════════════════════════════════════════
    # TRANSFORM 3: Map
    # ═══════════════════════════════════════════════════════════
    # Apply a function to EACH row — modify/add/remove fields
    def clean_row(record):
        """Process each record individually"""
        # Normalize email
        if record["email"]:
            record["email"] = record["email"].lower().strip()

        # Add derived field
        if record["quantity"] and record["unit_price"]:
            record["revenue"] = float(record["quantity"]) * float(record["unit_price"])
        else:
            record["revenue"] = 0.0

        # Remove unwanted field
        if "_extraction_timestamp" in record:
            del record["_extraction_timestamp"]

        return record

    mapped_rows = Map.apply(
        frame=dyf,
        f=clean_row,
        transformation_ctx="map_clean"
    )

    # ═══════════════════════════════════════════════════════════
    # TRANSFORM 4: ResolveChoice
    # ═══════════════════════════════════════════════════════════
    # When a column has multiple types across records (ChoiceType)
    # Example: "price" is string in some files, double in others
    # Glue reads it as: price: choice {string, double}
    #
    # STRATEGIES:
    # "cast:double"    → cast everything to double
    # "make_cols"      → create price_string AND price_double columns
    # "make_struct"    → create struct with both values
    # "project:double" → keep only double values, drop strings
    resolved = ResolveChoice.apply(
        frame=dyf,
        choice="cast:double",         # Cast all variants to double
        transformation_ctx="resolve_choice"
    )

    # Per-column resolution
    resolved_specific = ResolveChoice.apply(
        frame=dyf,
        specs=[
            ("price", "cast:double"),
            ("quantity", "cast:int"),
            ("is_active", "cast:boolean")
        ],
        transformation_ctx="resolve_specific"
    )

    # ═══════════════════════════════════════════════════════════
    # TRANSFORM 5: DropNullFields
    # ═══════════════════════════════════════════════════════════
    # Remove columns that are ALL NULL (every row is NULL)
    # Useful for cleaning sparse datasets
    no_null_cols = DropNullFields.apply(
        frame=dyf,
        transformation_ctx="drop_null_fields"
    )

    # ═══════════════════════════════════════════════════════════
    # TRANSFORM 6: DropFields / SelectFields
    # ═══════════════════════════════════════════════════════════
    # Drop specific columns
    dropped = DropFields.apply(
        frame=dyf,
        paths=["_extraction_timestamp", "_source_system", "year", "month", "day"],
        transformation_ctx="drop_fields"
    )

    # Keep only specific columns (opposite of DropFields)
    selected = SelectFields.apply(
        frame=dyf,
        paths=["order_id", "customer_id", "total_amount", "status"],
        transformation_ctx="select_fields"
    )

    # ═══════════════════════════════════════════════════════════
    # TRANSFORM 7: RenameField
    # ═══════════════════════════════════════════════════════════
    renamed = RenameField.apply(
        frame=dyf,
        old_name="customer_id",
        new_name="cust_id",
        transformation_ctx="rename_field"
    )

    # ═══════════════════════════════════════════════════════════
    # TRANSFORM 8: Join (DynamicFrame version)
    # ═══════════════════════════════════════════════════════════
    # Join two DynamicFrames
    # NOTE: PySpark DataFrame join is more flexible
    # DynamicFrame join only supports equality conditions
    orders_dyf = dyf  # Placeholder
    products_dyf = dyf  # Placeholder

    joined = Join.apply(
        frame1=orders_dyf,
        frame2=products_dyf,
        keys1=["product_id"],
        keys2=["product_id"],
        transformation_ctx="join_orders_products"
    )

    # ═══════════════════════════════════════════════════════════
    # TRANSFORM 9: Relationalize (FLATTEN nested structures)
    # ═══════════════════════════════════════════════════════════
    # Converts nested JSON/struct into flat relational tables
    # Input:
    #   {"order_id": "O1", "items": [{"sku": "A", "qty": 2}, {"sku": "B", "qty": 1}]}
    # Output:
    #   Root table: {"order_id": "O1", "items": 1}  (reference ID)
    #   Items table: {"items.val.sku": "A", "items.val.qty": 2, "id": 1}
    #                {"items.val.sku": "B", "items.val.qty": 1, "id": 1}

    flat_collection = Relationalize.apply(
        frame=dyf,
        staging_path="s3://bucket/tmp/relationalize/",  # Temp storage for large arrays
        name="root",                                      # Name prefix for output tables
        transformation_ctx="relationalize"
    )

    # flat_collection is a DynamicFrameCollection
    # Access individual tables:
    root_table = flat_collection.select("root")
    items_table = flat_collection.select("root_items")

    # ═══════════════════════════════════════════════════════════
    # TRANSFORM 10: SplitRows / SplitFields
    # ═══════════════════════════════════════════════════════════
    # Split DynamicFrame into two based on predicate
    split_result = SplitRows.apply(
        frame=dyf,
        comparison_dict={
            "total_amount": {
                ">": 100    # Split: > 100 vs <= 100
            }
        },
        transformation_ctx="split_rows"
    )

    high_value_orders = split_result.select("total_amount_>_100")
    low_value_orders = split_result.select("total_amount_<=_100")

    # ═══════════════════════════════════════════════════════════
    # TRANSFORM 11: Coalesce / Repartition
    # ═══════════════════════════════════════════════════════════
    # Control output file count
    # Must convert to DataFrame, coalesce, convert back
    df = dyf.toDF()
    df_coalesced = df.coalesce(4)  # 4 output files
    dyf_coalesced = DynamicFrame.fromDF(df_coalesced, glueContext, "coalesced")

    # ═══════════════════════════════════════════════════════════
    # CONVERSION: DynamicFrame ↔ DataFrame
    # ═══════════════════════════════════════════════════════════
    # DynamicFrame → DataFrame (for Spark operations)
    spark_df = dyf.toDF()

    # DataFrame → DynamicFrame (for Glue operations)
    glue_dyf = DynamicFrame.fromDF(spark_df, glueContext, "from_spark")

    return mapped, filtered, resolved