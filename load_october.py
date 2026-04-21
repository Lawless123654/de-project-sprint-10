import vertica_python
import psycopg2
from datetime import datetime, timedelta

pg_conn = psycopg2.connect(
    host="rc1b-w5d285tmxa8jimyn.mdb.yandexcloud.net",
    port=6432,
    dbname="db1",
    user="student",
    password="de_student_112022",
    sslmode="require"
)

vertica_conn = vertica_python.connect(
    host="vertica.data-engineer.education-services.ru",
    port=5433,
    user="vt260112e95f7e",
    password="fca2b217b0ed4f8e959152ac6a7a032e",
    database="dwh"
)

start_date = datetime(2022, 10, 1)
end_date = datetime(2022, 10, 31)
current = start_date

pg_cursor = pg_conn.cursor()
vertica_cursor = vertica_conn.cursor()

while current <= end_date:
    date_str = current.strftime("%Y-%m-%d")
    print(f"Processing {date_str}")

    pg_cursor.execute(f"""
        SELECT operation_id, account_number_from, account_number_to,
               currency_code, country, status, transaction_type,
               amount, transaction_dt
        FROM public.transactions
        WHERE DATE(transaction_dt) = \047{date_str}\047
    """)
    rows = pg_cursor.fetchall()
    if rows:
        vertica_cursor.executemany("""
            INSERT INTO VT260112E95F7E__STAGING.transactions
            (operation_id, account_number_from, account_number_to,
             currency_code, country, status, transaction_type,
             amount, transaction_dt, loaded_ts)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """, rows)
        print(f"  Loaded {len(rows)} transactions")

    pg_cursor.execute(f"""
        SELECT date_update, currency_code, currency_code_with, currency_with_div
        FROM public.currencies
        WHERE date_update = \047{date_str}\047
    """)
    rows = pg_cursor.fetchall()
    if rows:
        vertica_cursor.executemany("""
            INSERT INTO VT260112E95F7E__STAGING.currencies
            (date_update, currency_code, currency_code_with, currency_code_div, loaded_ts)
            VALUES (%s, %s, %s, %s, NOW())
        """, rows)
        print(f"  Loaded {len(rows)} currency rows")

    vertica_conn.commit()
    current += timedelta(days=1)

pg_cursor.close()
pg_conn.close()
vertica_cursor.close()
vertica_conn.close()
print("All data loaded successfully!")
