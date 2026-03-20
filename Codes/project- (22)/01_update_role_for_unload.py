# file: 01_update_role_for_unload.py
# Adds s3:PutObject permission to the existing Redshift role
# Role was created in Project 21 with only s3:GetObject

import boto3
import json

IAM_ROLE_NAME = "RedshiftS3ReadRole"  # Same role from Project 21
BUCKET_NAME = "quickcart-raw-data-prod"


def add_write_permission():
    """
    Update the existing Redshift IAM role to include S3 write access
    
    WHY update existing role vs create new?
    → Simpler: one role attached to cluster handles both COPY and UNLOAD
    → Redshift cluster can have max 10 IAM roles
    → Less to manage and audit
    
    NOTE: Rename role to "RedshiftS3Role" would be ideal
    → But renaming IAM roles is not possible in AWS
    → Just update the policy — the name is cosmetic
    """
    iam_client = boto3.client("iam")

    # Updated policy: read + write
    s3_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "S3ReadForCopy",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    f"arn:aws:s3:::{BUCKET_NAME}",
                    f"arn:aws:s3:::{BUCKET_NAME}/*"
                ]
            },
            {
                "Sid": "S3WriteForUnload",
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                    "s3:DeleteObject",      # Needed for CLEANPATH
                    "s3:GetBucketLocation"  # Needed for UNLOAD region detection
                ],
                "Resource": [
                    f"arn:aws:s3:::{BUCKET_NAME}",
                    f"arn:aws:s3:::{BUCKET_NAME}/exports/*"  # Restrict writes to exports/ prefix
                ]
            }
        ]
    }

    iam_client.put_role_policy(
        RoleName=IAM_ROLE_NAME,
        PolicyName="S3ReadWriteAccess",
        PolicyDocument=json.dumps(s3_policy)
    )

    print(f"✅ Updated {IAM_ROLE_NAME} with S3 write permissions")
    print(f"   Read:  s3://{BUCKET_NAME}/* (entire bucket)")
    print(f"   Write: s3://{BUCKET_NAME}/exports/* (exports prefix only)")
    print(f"\n💡 Write restricted to 'exports/' prefix — defense in depth")
    print(f"   UNLOAD cannot accidentally overwrite raw data files")


if __name__ == "__main__":
    add_write_permission()