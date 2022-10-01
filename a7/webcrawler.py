from pyspark import SparkContext, SparkConf
import requests, re

baseUrl = "https://hari1500.github.io/CS387-lab7-crawler-website"
startPaths = [("/1.html", 1)]

def webcrawl(url):
    # try:
    headers = requests.head(baseUrl + url[0]).headers
    if "html" in headers["content-type"]:
        res = requests.get(baseUrl + url[0])
        newurls = re.findall('<a href="(.*)"', res.text)
        filtered = filter(lambda x: False if x.startswith("http") else True, newurls)
        return [('/'+u, 1) for u in filtered]
    else:
        return []
    # except Exception as e:
    #     print("ERROR: CRAWLING: ",e)
    #     return []


if __name__ == "__main__":
    conf = SparkConf().setAppName("Web Crawler").setMaster("local")
    sc = SparkContext(conf=conf)

    total = sc.parallelize(startPaths)
    new = total
    newcount = 1

    while newcount > 0:
        # input("start")
        # crawl one time
        tempRdd = new.flatMap(webcrawl).reduceByKey(int.__add__)
        # get new urls in diff
        new = tempRdd.subtractByKey(total)
        # take union of result and reduce by key
        total = total.union(tempRdd).reduceByKey(int.__add__)

        newcount = new.count()
        # print("newcount", newcount, "total", total.count(), "temp", tempRdd.count())

        # total.cache()
        # new.cache()
        # input("done")
    
    total.coalesce(1)
    total.saveAsTextFile("./webcrawler/")

