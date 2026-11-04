"""Read-only checks for pipeline output; prints aggregate counts only."""
import os
from databricks import sql

failures = []
counts = {}
catalog = os.environ.get('DATABRICKS_CATALOG', 'walmart').replace('`', '``')
def table(schema, name):
    return f'`{catalog}`.`{schema}`.`{name}`'

with sql.connect(
    server_hostname=os.environ['DATABRICKS_HOST'].removeprefix('https://').rstrip('/'),
    http_path=os.environ['DATABRICKS_HTTP_PATH'],
    access_token=os.environ['DATABRICKS_TOKEN'],
) as connection:
    with connection.cursor() as cursor:
        def count(label, query):
            cursor.execute(query)
            value = cursor.fetchone()[0]
            print(f'{label}: {value}', flush=True)
            counts[label] = value
            if ('duplicate' in label or 'null relationship' in label) and value:
                failures.append(label)
            return value
        for name in ('orders_t', 'order_items_t', 'customers_t', 'products_t', 'employees_t', 'stores_t'):
            count(name, f'SELECT COUNT(*) FROM {table("silver_t", name)}')
        for name in ('obt_b',):
            count(name, f'SELECT COUNT(*) FROM {table("silver_b", name)}')
        fact = table('gold', 'fact_orders')
        count('fact_orders', f'SELECT COUNT(*) FROM {fact}')
        count('fact duplicate order-item keys', f'SELECT COUNT(*) FROM (SELECT order_item_id FROM {fact} GROUP BY order_item_id HAVING COUNT(*) > 1)')
        count('fact null relationship keys', f'SELECT COUNT(*) FROM {fact} WHERE order_item_id IS NULL OR order_id IS NULL OR customer_id IS NULL OR product_id IS NULL OR store_id IS NULL')
        for name, keys in [('dim_orders','order_item_id'), ('dim_customers','customer_id'), ('dim_products','product_id'), ('dim_stores','store_id'), ('dim_employees','employee_id, store_id')]:
            relation = table('gold', name)
            count(name, f'SELECT COUNT(*) FROM {relation}')
            count(name + ' duplicate current keys', f"SELECT COUNT(*) FROM (SELECT {keys} FROM {relation} WHERE dbt_valid_to = to_date('9999-12-31') GROUP BY {keys} HAVING COUNT(*) > 1)")

if counts['fact_orders'] != counts['order_items_t']:
    failures.append('fact count does not match source order-item count')
if failures:
    raise SystemExit('FAILED: ' + ', '.join(failures))
print('All warehouse aggregate checks passed.')
