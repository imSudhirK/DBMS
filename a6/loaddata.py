import sys, csv

#creating cypher.json for output cyper code  
f1 = open('cypher.sql', 'w+')

#generating nodes for users.csv
#CREATE (u1:User {id: 1, name: "James"});
with open('users.csv', 'r') as users_csv:
	users_reader = csv.reader(users_csv, delimiter=',')
	next(users_reader)
	for row in users_reader:
		f1.write("CREATE (u"+row[0]+":User {id: "+row[0]+", name: "+'"'+row[1]+'"'+"}); \n")
users_csv.close()

#generating nodes for tweets.csv  
#CREATE (t3:Tweet {id: 3, tweet: "bihday your majesty"});  
with open('tweets.csv', 'r') as tweets_csv:
	tweets_reader = csv.reader(tweets_csv, delimiter=',')
	next(tweets_reader)
	for row in tweets_reader:
		f1.write("CREATE (t"+row[0]+":Tweet {id: "+row[0]+", tweet: "+'"'+row[1]+'"'+"}); \n")
tweets_csv.close()

#generating nodes for hastags.csv 
#CREATE (h1:Hashtag {id: 1, hashtag: "run"}); 
with open('hashtags.csv', 'r') as hashtags_csv:
	hashtags_reader = csv.reader(hashtags_csv, delimiter=',')
	next(hashtags_reader)
	for row in hashtags_reader:
		f1.write("CREATE (h"+row[0]+":Hashtag {id: "+row[0]+", hashtag: "+'"'+row[1]+'"'+"}); \n")
hashtags_csv.close()

#generating relationship Follows 
#MATCH (u2:User{id: 2}), (u7:User{id: 7}) CREATE (u2)-[r:Follows]->(u7);
with open('follows.csv', 'r') as follows_csv:
	follows_reader = csv.reader(follows_csv, delimiter=',')
	next(follows_reader)
	for row in follows_reader:
		f1.write("MATCH (u"+row[0]+":User{id: "+row[0]+"}), (u"+row[1]+":User{id: "+row[1]+"}) CREATE (u"+row[0]+")-[r:Follows]->(u"+row[1]+"); \n")
follows_csv.close()

#generating relationship Sent
#MATCH (u9:User{id: 9}), (t9:Tweet{id: 9}) CREATE (u9)-[r:Sent]->(t9);
with open('sent.csv', 'r') as sent_csv:
	sent_reader = csv.reader(sent_csv, delimiter=',')
	next(sent_reader)
	for row in sent_reader:
		f1.write("MATCH (u"+row[0]+":User{id: "+row[0]+"}), (t"+row[1]+":Tweet{id: "+row[1]+"}) CREATE (u"+row[0]+")-[r:Sent]->(t"+row[1]+"); \n")
sent_csv.close()

#generating relationship Mentions
#MATCH (t25:Tweet{id: 25}), (u1:User{id: 1}) CREATE (t25)-[r:Mentions]->(u1);
with open('mentions.csv', 'r') as mentions_csv:
	mentions_reader = csv.reader(mentions_csv, delimiter=',')
	next(mentions_reader)
	for row in mentions_reader:
		f1.write("MATCH (t"+row[0]+":Tweet{id: "+row[0]+"}), (u"+row[1]+":User{id: "+row[1]+"}) CREATE (t"+row[0]+")-[r:Mentions]->(u"+row[1]+"); \n")
mentions_csv.close()

#generating relationship Contains
#MATCH (t32:Tweet{id: 32}), (h81:Hashtag{id: 81}) CREATE (t32)-[r:Contains]->(h81);
with open('contains.csv', 'r') as contains_csv:
	contains_reader = csv.reader(contains_csv, delimiter=',')
	next(contains_reader)
	for row in contains_reader:
		f1.write("MATCH (t"+row[0]+":Tweet{id: "+row[0]+"}), (h"+row[1]+":Hashtag{id: "+row[1]+"}) CREATE (t"+row[0]+")-[r:Contains]->(h"+row[1]+"); \n")
contains_csv.close()
