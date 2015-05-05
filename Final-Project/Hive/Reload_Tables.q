create external table if not exists retweetersForFinal(retweetee string, retweeter string)
PARTITIONED BY(retweetee_char string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE LOCATION 's3://twittermr/tables/retweetersForFinal';

create external table if not exists network(follower string, friend string)
PARTITIONED BY(follower_char string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE LOCATION 's3://twittermr/tables/network/';

create external table if not exists users(user string, name string, friend_ct string, follower_ct int, status_ct string, favorite_ct string, age string, location string )
PARTITIONED BY(user_char string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE LOCATION 's3://twittermr/tables/users';

create external table if not exists retweetCount(retweetee string, num_of_retweets decimal)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION 's3://twittermr/tables/retweetCount';

msck repair table network;
msck repair table users;
msck repair table retweetersForFinal;