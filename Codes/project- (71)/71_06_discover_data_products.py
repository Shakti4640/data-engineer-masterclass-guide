# file: 71_06_discover_data_products.py
# Any account can query the registry to discover available data products
# Runs from Governance account or via cross-account role

import boto3
from boto3.dynamodb.conditions import Key, Attr

REGION = "us-east-2"
TABLE_NAME = "mesh-data-product-registry"


def list_all_products(table):
    """Scan entire registry — use for small catalogs only"""
    response = table.scan()
    items = response["Items"]

    print(f"\n📚 DATA PRODUCT CATALOG — {len(items)} products registered")
    print("=" * 90)
    print(f"{'Domain':<12} {'Product':<25} {'Ver':<5} {'Status':<10} "
          f"{'Quality':<10} {'SLA (hrs)':<10} {'Last Published':<22}")
    print("-" * 90)

    for item in sorted(items, key=lambda x: x["domain_product"]):
        print(f"{item['domain']:<12} "
              f"{item['product_name']:<25} "
              f"{item['version']:<5} "
              f"{item['status']:<10} "
              f"{str(item.get('last_quality_score', 'N/A')):<10} "
              f"{str(item.get('sla_freshness_hours', 'N/A')):<10} "
              f"{item.get('last_published', 'Never'):<22}")

    print("=" * 90)


def search_by_domain(table, domain):
    """Find all products from a specific domain"""
    response = table.query(
        IndexName="domain-index",
        KeyConditionExpression=Key("domain").eq(domain)
    )

    items = response["Items"]
    print(f"\n🔍 Products from domain: {domain} ({len(items)} found)")
    for item in items:
        print(f"   → {item['product_name']} v{item['version']} "
              f"[{item['status']}] — {item.get('description', '')[:60]}")


def get_product_details(table, domain, product_name, version):
    """Get full details of a specific data product"""
    response = table.get_item(
        Key={
            "domain_product": f"{domain}#{product_name}",
            "version": version
        }
    )

    if "Item" not in response:
        print(f"❌ Product not found: {domain}#{product_name} v{version}")
        return

    item = response["Item"]
    print(f"\n📋 DATA PRODUCT DETAILS")
    print("=" * 60)
    for key, value in sorted(item.items()):
        if isinstance(value, list):
            value = ", ".join(value)
        print(f"   {key:<30} {value}")
    print("=" * 60)


def find_stale_products(table, max_hours=24):
    """Find products that haven't been published within SLA"""
    from datetime import datetime, timedelta

    response = table.scan(
        FilterExpression=Attr("status").eq("active")
    )

    cutoff = datetime.utcnow() - timedelta(hours=max_hours)
    stale = []

    for item in response["Items"]:
        last_pub = item.get("last_published")
        if last_pub:
            pub_time = datetime.fromisoformat(last_pub.replace("Z", "+00:00")).replace(tzinfo=None)
            if pub_time < cutoff:
                stale.append(item)

    if stale:
        print(f"\n⚠️  STALE DATA PRODUCTS (not published in {max_hours}h):")
        for item in stale:
            print(f"   → {item['domain']}.{item['product_name']} "
                  f"v{item['version']} — last: {item['last_published']}")
    else:
        print(f"\n✅ All active products published within {max_hours}h")

    return stale


if __name__ == "__main__":
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb.Table(TABLE_NAME)

    # Discover all products
    list_all_products(table)

    # Search by domain
    search_by_domain(table, "orders")

    # Get specific product details
    get_product_details(table, "orders", "daily_orders", "v2")

    # Find stale products
    find_stale_products(table, max_hours=12)