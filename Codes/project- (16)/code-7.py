{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AthenaQueryAccess",
            "Effect": "Allow",
            "Action": [
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults",
                "athena:StopQueryExecution",
                "athena:ListQueryExecutions",
                "athena:GetWorkGroup",
                "athena:CreateWorkGroup"
            ],
            "Resource": [
                "arn:aws:athena:us-east-2:ACCOUNT_ID:workgroup/primary",
                "arn:aws:athena:us-east-2:ACCOUNT_ID:workgroup/quickcart-analysts"
            ]
        },
        {
            "Sid": "GlueCatalogAccess",
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:GetTable",
                "glue:GetTables",
                "glue:GetPartition",
                "glue:GetPartitions",
                "glue:CreateDatabase",
                "glue:CreateTable",
                "glue:DeleteTable",
                "glue:DeleteDatabase"
            ],
            "Resource": [
                "arn:aws:glue:us-east-2:ACCOUNT_ID:catalog",
                "arn:aws:glue:us-east-2:ACCOUNT_ID:database/quickcart_raw",
                "arn:aws:glue:us-east-2:ACCOUNT_ID:table/quickcart_raw/*"
            ]
        },
        {
            "Sid": "S3SourceDataReadAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::quickcart-raw-data-prod",
                "arn:aws:s3:::quickcart-raw-data-prod/*"
            ]
        },
        {
            "Sid": "S3AthenaResultsWriteAccess",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket",
                "s3:GetBucketLocation",
                "s3:AbortMultipartUpload",
                "s3:ListMultipartUploadParts"
            ],
            "Resource": [
                "arn:aws:s3:::quickcart-athena-results",
                "arn:aws:s3:::quickcart-athena-results/*"
            ]
        }
    ]
}