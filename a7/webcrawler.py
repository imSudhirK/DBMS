import requests
import sys, re, os 

from pyspark import SparkContext, SparkConf


def download(url, file):
	req = requests.get(url)
	with open(file, "wb") as f1:
		f1.write(req.content)

def updateList(page, List):
	p = open(page)
	pc = p.read()
	match = re.findall(r'<a href="(.*?)">', pc)
	for link in match:
		if match:
			List.append(link)

def updateCSV(page, f2):
	p = open(page)
	pc = p.read()
	match = re.findall(r'<a href="(.*?)">', pc)
	for link in match:
		if match:
			f2.write('/'+link +'\n')


url0 = 'https://hari1500.github.io/CS387-lab7-crawler-website/1.html'
download(url0, '1.html')

bcsv = open('Base.csv', 'w+')
updateCSV('1.html', bcsv)

baseList =[]
currList = []
itrList=[]

updateList('1.html', currList)
baseList = list(set(currList))
itrList = list(set(currList))

while True:
	for link in itrList:
		url_x = 'https://hari1500.github.io/CS387-lab7-crawler-website/' + link
		h = requests.head(url_x)
		header = h.headers
		content_type = header.get('content-type')
		if "text/html" in content_type:
			page0 = link
			download(url_x, page0)
			updateList(page0, currList)
		else:
			bcsv.write('/'+link+'\n')

	for e in currList:
		bcsv.write('/'+e +'\n')

	itrList = list(set(currList)- set(baseList))

	for e in list(set(currList)):
		baseList.append(e)

	currList = []
	if itrList == []:
		break


if __name__ == "__main__":
	sc = SparkContext("local","WC")
	words = sc.textFile("Base.csv").flatMap(lambda line: line.split(" "))
	wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
	wordCounts.saveAsTextFile("./webcrawler/")
	os.remove("Base.csv")
