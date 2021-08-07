import sys, csv
import psycopg2, time 

if __name__ == "__main__":
    from sys import argv
    try:
        conn1 = psycopg2.connect( host="127.0.0.1", port=5432, dbname="lab4db", user="postgres", password="postgres")
        curr1 = conn1.cursor()

        curr1.execute("select * from outlab4")

        n = int(input("Enter number of rows to be printed at once(negative to exit): "))
        n0 = n
        if n<0:
            conn1.close()
        while n < 100000:
            t0 = time.time()
            ndata = curr1.fetchmany(n0)
            #for line in ndata:
            #   print(line)
            print(str(n-n0) + "-" + str(n) + " rows " + "--%s sec--" % (time.time() - t0))
            n +=n0  
        
    except Exception as err:
        print("ERROR %%%%%%%%%%%%%%%% \n", err)

#this time since whatever time we take in connection but 
#for selction and printing 100 rows is  not incresing its varying around some average time 
