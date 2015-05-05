create external table if not exists FindTweetersFollowers(tweeter string, total_retweets decimal, total_followers int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION 's3://twittermr/tables/tweeters_retweets_followers';

INSERT OVERWRITE TABLE FindTweetersFollowers
SELECT retweetee, num_of_retweets, follower_ct FROM retweetCount JOIN users ON retweetee = user;
