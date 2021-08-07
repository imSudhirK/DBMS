import psycopg2
import random 
import psycopg2, config

if __name__ == "__main__":
    from sys import argv
    import config
    sql_table =(
        """
        CREATE TABLE oracle(
            Query INT,
            output VARCHAR
        );
        """
    )

    try:
        conn1 = psycopg2.connect( host="127.0.0.1", port=5432, dbname="lab4db", user="postgres", password="postgres")
        curr1 = conn1.cursor()
        curr1.execute("""DROP TABLE IF EXISTS oracle;""")
        curr1.execute(sql_table)
        conn1.commit()
        num = str(input("Query:\n"))
        while num != -1:
            rand_num = random.getrandbits(256)
            string1 = hex(rand_num)
            sql_bool =(
                """
                SELECT Query from oracle
                WHERE  EXISTS(SELECT Query FROM oracle where Query = num);
                """
            )
            curr1.execute(sql_bool)
            curr1.execute("INSERT INTO oracle(Query, output) values (num, string1);")
            conn1.commit()
            num = int(input("Query:\n"))
            
        if num == -1:
            print("exiting...")
        conn1.close()
    except Exception as err:
        print("ERROR %%%%%%%%%%%%%%%% \n", err)
