import sys, csv 
import pandas as pd 


#dropping rows with null columns values
#it will take some time(4-5min) to parse approx(8405080 rows) data 
#to create newfile.csv of approx(1386646 rows)
covid_csv = pd.read_csv('COVID-19_Case_Surveillance_Public_Use_Data.csv')
covid_csv.dropna(axis=0, how = 'any', inplace =True)
covid_csv.to_csv('newfile.csv', index = False)


#Partitioning data into 10 smaller parts data1---10.csv
base_csv = open('newfile.csv', 'r').readlines()
x=1
y = 100000
for i in range(len(base_csv)):
	if i % y == 0:
		open('data' + str(x) + '.csv', 'w+').writelines(base_csv[i:i+y])
		x +=1
		y +=100000


#again it will take some 1-2 min 
#generating sql file of above data1-10.csv into data1-10.sql respectively
#table created is outlab4 
for i in range(1, 11):
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


#for loading I am just copying these files(data1.sql---data10.sql)
#using sudo docker cp ~/path/data1-10.sql containerid:/home
#to docker and then loading using \i /home/data1.sql 
#here are the size of files and time it takes 
#for data1.sql(100000 rows)  real time 22min 
#for data2.sql(200000 rows)  real time 47min
#for other time file calculated using average from time for each rows inserting(20 rows only)
#average is around 10.935 ms for each row but also have some outliers (eg. 34.467 ms ignored) 