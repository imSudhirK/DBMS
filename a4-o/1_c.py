import psycopg2
import sys, csv 
import time 

#Partitioning data into 5 smaller parts data1---5.csv  from given newfile.csv
t0 = time.time()
base_csv = open('newfile.csv', 'r').readlines()
x=1
y = 200000
for i in range(len(base_csv)):
	if i % y == 0:
		open('data' + str(x) + '.csv', 'w+').writelines(base_csv[i:i+y])
		x +=1
		y +=200000
print("--%s sec--" % (time.time() - t0))

#bulk loading of each data1--5.csv recursively 
if __name__ == "__main__":
    from sys import argv

    try:
        conn1 = psycopg2.connect( host="127.0.0.1", port=5432, dbname="lab4db", user="postgres", password="postgres")
        curr1 = conn1.cursor()

        sql_table =(
            """
            create table outlab4(cdc_report_dt varchar, pos_spec_dt varchar,
            onset_dt varchar, current_status varchar, sex varchar, age_group varchar,
            Race_and_ethnicity varchar, hosp_yn varchar, icu_yn varchar, 
            death_yn varchar, medcond_yn varchar);
            """
        )
        curr1.execute("""DROP TABLE IF EXISTS outlab4;""")
        curr1.execute(sql_table)
        conn1.commit()

        for i in range(1, 6):
        	t0 = time.time()
        	with open('data' + str(i) + '.csv', mode='r') as data:
        	    curr1.copy_from(data, 'outlab4', sep=',')
        	    curr1.commit()
        	print("--%s sec--" % (time.time() - t0))
        conn1.close()
    except Exception as err:
        print("ERROR %%%%%%%%%%%%%%%% \n", err)