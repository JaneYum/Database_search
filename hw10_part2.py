import sqlite3

DB_NAME = 'tweets.sqlite'

try:
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
except Error as e:
    print(e)

def get_most_retweeted_tweet():
    #prints the tweet with the most number of retweets (just tweet text will do)
    print("-Get the tweet with the most number of retweets: ")
    statement = 'SELECT TweetText FROM Tweets Order By RetweetCount DESC LIMIT 1'
    cur.execute(statement)
    for row in cur:
        print(row[0].decode())

def get_most_followed_user():
    #print the user’s screen name who is most followed
    print("-Get user’s screen name who is most followed: ")
    statement = 'SELECT ScreenName FROM Tweets Order By FollowerCount DESC LIMIT 1'
    cur.execute(statement)
    for row in cur:
        print(row[0])

def get_most_retweeted_user():
    #print the user’s screen name who’s tweet had the highest retweet count
    print("-Get user’s screen name who’s tweet had the highest retweet count: ")
    statement = 'SELECT ScreenName FROM Tweets Order By RetweetCount DESC LIMIT 1'
    cur.execute(statement)
    for row in cur:
        print(row[0])

def get_tweets_from_most_followed():
    #print the 5 tweets that belong to authors with highest number of followers. Order this in the the descending order.
    print("-Get 5 tweets that belong to authors with highest number of followers: ")
    statement = 'SELECT TweetText FROM Tweets Order By FollowerCount DESC LIMIT 5'
    cur.execute(statement)
    i = 0
    for row in cur:
        i +=1
        print(i, row[0].decode())

def get_trending_location():
    #print the top 5 locations that are tweeting about us/ Going Blue. Order in the descending order.
    print("-Get top 5 locations that are tweeting about us/ Going Blue: ")
    statement = 'SELECT Location, COUNT(*) FROM Tweets GROUP BY Location HAVING TweetText LIKE "%Go Blue%" Order By COUNT(*) DESC LIMIT 5'
    cur.execute(statement)
    for row in cur:
        print(row[0])

get_most_retweeted_tweet()
get_tweets_from_most_followed()
get_most_followed_user()
get_most_retweeted_user()
get_trending_location()
