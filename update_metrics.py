import vertica_python
from datetime import datetime, timedelta

vertica_conn = vertica_python.connect(
    host="vertica.data-engineer.education-services.ru",
    port=5433,
    user="vt260112e95f7e",
    password="fca2b217b0ed4f8e959152ac6a7a032e",
    database="dwh"
)

cursor = vertica_conn.cursor()

start_date = datetime(2022, 10, 1)
end_date = datetime(2022, 10, 31)
current = start_date

while current <= end_date:
    date_str = current.strftime("%Y-%m-%d")
    print(f"Updating metrics for {date_str}")

    cursor.execute(f"""
        INSERT INTO VT260112E95F7E__DWH.global_metrics
        (date_update, currency_from, amount_total, cnt_transactions,
         avg_transactions_per_account, cnt_accounts_make_transactions)
        SELECT
            DATE(t.transaction_dt) AS date_update,
            t.currency_code AS currency_from,
            SUM(t.amount * COALESCE(c.currency_code_div, 1)) AS amount_total,
            COUNT(*) AS cnt_transactions,
            COUNT(*) / NULLIF(COUNT(DISTINCT t.account_number_from), 0) AS avg_transactions_per_account,
            COUNT(DISTINCT t.account_number_from) AS cnt_accounts_make_transactions
        FROM VT260112E95F7E__STAGING.transactions t
        LEFT JOIN VT260112E95F7E__STAGING.currencies c
            ON t.currency_code = c.currency_code
            AND c.currency_code_with = 840
            AND c.date_update = DATE(t.transaction_dt)
        WHERE t.account_number_from > 0
          AND DATE(t.transaction_dt) = \047{date_str}\047
        GROUP BY 1, 2
    """)
    print(f"  Rows inserted: {cursor.rowcount}")
    vertica_conn.commit()
    current += timedelta(days=1)

cursor.close()
vertica_conn.close()
print("All metrics updated successfully!")
