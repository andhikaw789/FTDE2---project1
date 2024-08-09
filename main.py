import connection
import os
import sqlparse
import pandas as pd

if __name__ == '__main__':
    conf = connection.config('marketplace_prod')
    conn, engine = connection.get_conn(conf, "DataSource")
    cursor = conn.cursor()


    conf_dwh = connection.config('dwh')
    conn_dwh, engine_dwh = connection.get_conn(conf_dwh, 'DWH')
    cursor_dwh = conn_dwh.cursor()


    path_query = os.getcwd() + '/query/'
    query = sqlparse.format(
        open(path_query + 'query.sql', 'r').read(), strip_comments = True
    ).strip()
    dwh_design = sqlparse.format(
        open(path_query + 'dwh_design.sql', 'r').read(), strip_comments = True
    ).strip()


    try:
        print("[INFO] service etl is running..")
        df = pd.read_sql(query, engine)

        cursor_dwh.execute(dwh_design)
        conn_dwh.commit()

        df.to_sql(
            'dim_orders_andhika',
            engine_dwh,
            schema = 'public',
            if_exists = 'append',
            index=False
        )
        print("service etl done!")
    except Exception as e:
        print("error")
        print(str(e))


    