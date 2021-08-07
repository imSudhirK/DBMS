import sys, csv
import psycopg2, time 

if __name__ == "__main__":
    from sys import argv
    try:
        conn1 = psycopg2.connect( host="127.0.0.1", port=5432, dbname="lab4db", user="postgres", password="postgres")
        curr1 = conn1.cursor()

        x=0
        n=int(input("Enter number of rows to iterate (negative to exit) : "))
        n0 = n
        if n<0:
            conn1.close()
        while n <100000:
            t0 = time.time()
            curr1.execute(" select * from outlab4 order by cdc_report_dt " + " offset " + str(x) + 
                " fetch next " + str(n) + " rows only ")
            ndata = curr1.fetchall()
            #for line in ndata:
            #    print(line)
            x +=n
            print(str(n-n0) + "-" + str(n) + " rows " + "--%s sec--" % (time.time() - t0))
            n +=n0
           
    except Exception as err:
        print("ERROR %%%%%%%%%%%%%%%% \n", err)

#I am printing run time for each iteration of 100 rows selection 
#and yes time is incresing every time 
