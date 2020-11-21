hiveData = LOAD '/user/hive/hiveTableContent' USING PigStorage(',');
CombineFields = FOREACH hiveData GENERATE $0, CONCAT($1, $2, $3) AS POSTS;
CleanPosts = FOREACH CombineFields GENERATE $0, REPLACE($1, '\\n|\\r|<br>|\\t|<.+?>|([^a-zA-Z\\s]+)',' ') AS POSTS;
STORE CleanPosts INTO '/user/hive/hiveClean3' USING PigStorage(',');
