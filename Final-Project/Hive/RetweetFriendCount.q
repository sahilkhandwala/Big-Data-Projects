create external table if not exists retweetFriendCount(retweetee string, num_of_retweets decimal)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION 's3://twittermr/tables/retweetFriendCount';

INSERT OVERWRITE TABLE retweetFriendCount
SELECT retweetee, count(*) FROM network, retweetersForFinal 
WHERE SUBSTR(network.follower, 1, 1) = SUBSTR(retweetersForFinal.retweeter, 1, 1) 
AND retweetersForFinal.retweetee = network.friend
AND retweetersForFinal.retweeter = network.follower
GROUP BY retweetee;create external table if not exists users_temp2(user string, name string, friend_ct string, follower_ct int, status_ct string, favorite_ct string, age string, location string )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION 's3://twittermr/tables/user_temp2';
