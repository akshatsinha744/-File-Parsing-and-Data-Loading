import psycopg2
import pandas as pd

hostname = 'localhost'
database = 'demo'
username = 'postgres'
pwd = '250857'
port_id = 5432
conn = None
cur = None

try:
    conn = psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id
    )
    cur = conn.cursor()

    file_path = r"C:\Users\Admin\Desktop\Akshat\question6_transaction_data.csv"
    df = pd.read_csv(file_path)


    table_name = 'q6_csv'
    create_script = f""" CREATE TABLE IF NOT EXISTS {table_name} (
                                transaction_id INT,
                                customer_id INT,
                                transaction_date DATE,
                                amount FLOAT);"""
    
    cur.execute(create_script)

    truncate_script = f"TRUNCATE TABLE {table_name};"
    cur.execute(truncate_script)


    with open(file_path, 'r') as file:
        cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", file)

    
    conn.commit()

    insert_script = ''' SELECT customer_id,SUM(amount) as total_spending
                        FROM q6_csv
                        GROUP BY customer_id
                        ORDER BY total_spending DESC'''
    
    cur.execute(insert_script)

    results = cur.fetchall()
    for row in results:
        print(row)
    

except Exception as error:
    print("Error:", error)

finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
