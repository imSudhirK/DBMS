import psycopg2, config

def connect():
    """ returns connection to database """
    # TODO: use variables from config file as connection params
    # conn = psycopg2.connect(....)
    # return conn
    conn = psycopg2.connect( host="127.0.0.1", port=5432, dbname="lab4db", user="postgres", password="postgres")
    return conn
	#curr = conn.cursor()
    pass

def exec_query(conn, sql):
    """ Executes sql query and returns header and rows """
    # TODO: create cursor, get header from cursor.description, and execute query to fetch rows.
    # return (header, rows)
    curr = conn.cursor()
    curr.execute(sql)
    header = [i[0] for i in curr.description]
    rows = curr.fetchall()
    return (header, rows)


    pass

if __name__ == "__main__":
    from sys import argv
    import config

    query = argv[1]
    try:
        conn = connect()
        (header, rows) = exec_query(conn, query)
        print(",".join([str(i) for i in header]))
        for r in rows:
            print(",".join([str(i) for i in r]))
        conn.close()
    except Exception as err:
        print("ERROR %%%%%%%%%%%%%%%% \n", err)
