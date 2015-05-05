create external table if not exists Combine(tweeter string, total_followers int, total_retweets decimal, num_of_retweets int, leftover decimal)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION 's3://twittermr/tables/combine1';

INSERT OVERWRITE TABLE Combine
SELECT tweeter, total_followers, total_retweets, num_of_retweets, (total_retweets - num_of_retweets) AS leftover FROM 
FindTweetersFollowers, retweetFriendCount WHERE retweetFriendCount.retweetee = FindTweetersFollowers.tweeter;


