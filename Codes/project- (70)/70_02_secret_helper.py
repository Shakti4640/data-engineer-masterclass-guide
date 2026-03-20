# file: 70_02_secret_helper.py
# Purpose: Reusable module for retrieving secrets from Secrets Manager
# Used by: ALL Glue jobs, Lambda functions, and scripts

import boto3
import json
from functools import lru_cache
from datetime import datetime, timedelta

_sm_client = None
_cache = {}
_cache_ttl = timedelta(hours=1)


def _get_client():
    """Lazy-initialize Secrets Manager client"""
    global _sm_client
    if _sm_client is None:
        _sm_client = boto3.client("secretsmanager")
    return _sm_client


def get_secret(secret_name, force_refresh=False):
    """
    Retrieve secret value from Secrets Manager with caching
    
    CACHING:
    → First call: fetches from Secrets Manager API
    → Subsequent calls within 1 hour: returns cached value
    → After 1 hour: re-fetches (picks up rotated credentials)
    → force_refresh=True: bypasses cache (use after rotation)
    
    RETURNS: dict with all secret fields (username, password, host, etc.)
    
    USAGE:
    secret = get_secret("quickcart/prod/mariadb/glue-reader")
    conn = pymysql.connect(
        host=secret["host"],
        user=secret["username"],
        password=secret["password"],
        database=secret["dbname"]
    )
    """
    global _cache

    # Check cache
    if not force_refresh and secret_name in _cache:
        cached_entry = _cache[secret_name]
        if datetime.utcnow() - cached_entry["fetched_at"] < _cache_ttl:
            return cached_entry["value"]

    # Fetch from Secrets Manager
    sm = _get_client()
    try:
        response = sm.get_secret_value(SecretId=secret_name)
        secret_value = json.loads(response["SecretString"])

        # Update cache
        _cache[secret_name] = {
            "value": secret_value,
            "fetched_at": datetime.utcnow(),
            "version_id": response.get("VersionId", "unknown")
        }

        # NEVER log the password — only log that we fetched it
        print(f"🔐 Secret retrieved: {secret_name} (version: {response.get('VersionId', 'unknown')[:8]}...)")

        return secret_value

    except Exception as e:
        print(f"❌ Failed to retrieve secret {secret_name}: {str(e)[:100]}")
        raise


def get_mariadb_connection_params():
    """Convenience: get MariaDB connection parameters"""
    secret = get_secret("quickcart/prod/mariadb/glue-reader")
    return {
        "host": secret["host"],
        "port": int(secret["port"]),
        "user": secret["username"],
        "password": secret["password"],
        "database": secret["dbname"]
    }


def get_redshift_connection_params():
    """Convenience: get Redshift connection parameters"""
    secret = get_secret("quickcart/prod/redshift/etl-writer")
    return {
        "host": secret["host"],
        "port": int(secret["port"]),
        "user": secret["username"],
        "password": secret["password"],
        "dbname": secret["dbname"]
    }


def get_redshift_data_api_params():
    """
    Convenience: get params for Redshift Data API
    Data API can use SecretArn directly — no password in code
    """
    sm = _get_client()
    # Get secret ARN (not the value — Data API fetches it internally)
    desc = sm.describe_secret(SecretId="quickcart/prod/redshift/etl-writer")
    return {
        "SecretArn": desc["ARN"],
        "Database": "analytics"
    }