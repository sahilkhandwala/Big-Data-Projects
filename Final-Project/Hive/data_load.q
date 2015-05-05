create external table if not exists users_temp(user string, name string, friend_ct decimal, follower_ct decimal, status_ct decimal, favorite_ct decimal, age string, location string )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION 's3://twittermr/tables/user_temp';

LOAD DATA INPATH 's3://twittermr/tables/users/users.txt' OVERWRITE INTO TABLE users_temp;

create external table if not exists users(user string, name string, friend_ct decimal, follower_ct decimal, status_ct decimal, favorite_ct decimal, age string, location string )
PARTITIONED BY(user_char string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE LOCATION 's3://twittermr/tables/users';

INSERT OVERWRITE TABLE users PARTITION(user_char='1') SELECT * FROM users_temp WHERE SUBSTR(user, 1, 1) = '1';
INSERT OVERWRITE TABLE users PARTITION(user_char='2') SELECT * FROM users_temp WHERE SUBSTR(user, 1, 1) = '2';
INSERT OVERWRITE TABLE users PARTITION(user_char='3') SELECT * FROM users_temp WHERE SUBSTR(user, 1, 1) = '3';
INSERT OVERWRITE TABLE users PARTITION(user_char='4') SELECT * FROM users_temp WHERE SUBSTR(user, 1, 1) = '4';
INSERT OVERWRITE TABLE users PARTITION(user_char='5') SELECT * FROM users_temp WHERE SUBSTR(user, 1, 1) = '5';
INSERT OVERWRITE TABLE users PARTITION(user_char='6') SELECT * FROM users_temp WHERE SUBSTR(user, 1, 1) = '6';
INSERT OVERWRITE TABLE users PARTITION(user_char='7') SELECT * FROM users_temp WHERE SUBSTR(user, 1, 1) = '7';
INSERT OVERWRITE TABLE users PARTITION(user_char='8') SELECT * FROM users_temp WHERE SUBSTR(user, 1, 1) = '8';
INSERT OVERWRITE TABLE users PARTITION(user_char='9') SELECT * FROM users_temp WHERE SUBSTR(user, 1, 1) = '9';

create external table if not exists network_temp(follower string, friend string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION 's3://twittermr/tables/network_temp';

LOAD DATA INPATH 's3://twittermr/tables/network/network.txt' OVERWRITE INTO TABLE network_temp;

create external table if not exists network(follower string, friend string)
PARTITIONED BY(follower_char string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE LOCATION 's3://twittermr/tables/network/';

INSERT OVERWRITE TABLE network PARTITION(follower_char='1') SELECT * FROM network_temp WHERE SUBSTR(follower, 1, 1) = '1';
INSERT OVERWRITE TABLE network PARTITION(follower_char='2') SELECT * FROM network_temp WHERE SUBSTR(follower, 1, 1) = '2';
INSERT OVERWRITE TABLE network PARTITION(follower_char='3') SELECT * FROM network_temp WHERE SUBSTR(follower, 1, 1) = '3';
INSERT OVERWRITE TABLE network PARTITION(follower_char='4') SELECT * FROM network_temp WHERE SUBSTR(follower, 1, 1) = '4';
INSERT OVERWRITE TABLE network PARTITION(follower_char='5') SELECT * FROM network_temp WHERE SUBSTR(follower, 1, 1) = '5';
INSERT OVERWRITE TABLE network PARTITION(follower_char='6') SELECT * FROM network_temp WHERE SUBSTR(follower, 1, 1) = '6';
INSERT OVERWRITE TABLE network PARTITION(follower_char='7') SELECT * FROM network_temp WHERE SUBSTR(follower, 1, 1) = '7';
INSERT OVERWRITE TABLE network PARTITION(follower_char='8') SELECT * FROM network_temp WHERE SUBSTR(follower, 1, 1) = '8';
INSERT OVERWRITE TABLE network PARTITION(follower_char='9') SELECT * FROM network_temp WHERE SUBSTR(follower, 1, 1) = '9';

create external table if not exists retweetersFor_temp(retweetee string, retweeter string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION 's3://twittermr/tables/rtf_temp';
LOAD DATA INPATH 's3://twittermr/GetRetweetersAndCountPerUser_new' OVERWRITE INTO TABLE retweetersFor_temp;

create external table if not exists retweetersForFinal(retweetee string, retweeter string)
PARTITIONED BY(retweetee_char string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE LOCATION 's3://twittermr/tables/retweetersForFinal';

INSERT OVERWRITE TABLE retweetersForFinal PARTITION(retweetee_char='1') SELECT * FROM retweetersFor_temp WHERE SUBSTR(retweetee, 1, 1) = '1';
INSERT OVERWRITE TABLE retweetersForFinal PARTITION(retweetee_char='2') SELECT * FROM retweetersFor_temp WHERE SUBSTR(retweetee, 1, 1) = '2';
INSERT OVERWRITE TABLE retweetersForFinal PARTITION(retweetee_char='3') SELECT * FROM retweetersFor_temp WHERE SUBSTR(retweetee, 1, 1) = '3';
INSERT OVERWRITE TABLE retweetersForFinal PARTITION(retweetee_char='4') SELECT * FROM retweetersFor_temp WHERE SUBSTR(retweetee, 1, 1) = '4';
INSERT OVERWRITE TABLE retweetersForFinal PARTITION(retweetee_char='5') SELECT * FROM retweetersFor_temp WHERE SUBSTR(retweetee, 1, 1) = '5';
INSERT OVERWRITE TABLE retweetersForFinal PARTITION(retweetee_char='6') SELECT * FROM retweetersFor_temp WHERE SUBSTR(retweetee, 1, 1) = '6';
INSERT OVERWRITE TABLE retweetersForFinal PARTITION(retweetee_char='7') SELECT * FROM retweetersFor_temp WHERE SUBSTR(retweetee, 1, 1) = '7';
INSERT OVERWRITE TABLE retweetersForFinal PARTITION(retweetee_char='8') SELECT * FROM retweetersFor_temp WHERE SUBSTR(retweetee, 1, 1) = '8';
INSERT OVERWRITE TABLE retweetersForFinal PARTITION(retweetee_char='9') SELECT * FROM retweetersFor_temp WHERE SUBSTR(retweetee, 1, 1) = '9';

create external table if not exists retweetCount(retweetee string, num_of_retweets decimal)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION 's3://twittermr/tables/retweetCount';
INSERT OVERWRITE TABLE retweetCount
SELECT retweetee, COUNT(*) as FROM retweetersForFinal GROUP BY retweetee;

quit;

