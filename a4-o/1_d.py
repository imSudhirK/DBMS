import sys, csv
from sys import argv
import psycopg2, time



#to generate sql file from data1---5.csv given
#takes 1-2min
t0 = time.time()
for i in range(1, 6):
	sqlfile1 = open('data' + str(i) + '.sql', 'w+')
	with open('data' + str(i) + '.csv', mode = 'r') as data1_csv:
		data1_reader = csv.reader(data1_csv, delimiter =',')
		next(data1_reader)
		for row in data1_reader:
			sqlfile1.write("INSERT INTO outlab4 VALUES (")
			for i in range(0, len(row)):
				sqlfile1.write("'" + row[i] + "'")
				if i+1< len(row) :
					sqlfile1.write(", ")
			sqlfile1.write(");\n")
	data1_csv.close()
	sqlfile1.close()

print("--%s sec--" % (time.time() - t0))


#inserting each tuple from generated sql file 
if __name__ == "__main__":
    try:
        conn1 = psycopg2.connect( host="127.0.0.1", port=5432, dbname="lab4db", user="postgres", password="postgres")
        curr1 = conn1.cursor()
        #creating table outlab4
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
        #populating outlab4 with data1--5.csv given 
        for i in range(1, 6):
            with open('data' + str(i) + '.sql', mode = 'r') as data1:
                for line in data1:
                	t1 = time.time()
                	curr1.execute(line)
                	conn1.commit()
                	print("--%s sec--" % (time.time() - t1)) 
        conn1.close()

    except Exception as err:
        print("ERROR %%%%%%%%%%%%%%%% \n", err)

#it will print time to insert sinlge tuple of data 
#i will take some 20 sample and take average 
#count number of rows in data using wc -l filename 
#multipy average time to num_rows
