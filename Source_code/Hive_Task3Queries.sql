--Hive Queries

hive> show databases;
hive> create database stack;
hive> use stack;

--Creating the table with all the attributes and picking up the the data from specified location

CREATE TABLE posts(Id INT, Score INT, Body STRING, OwnerUserId INT, Title STRING, Tags STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'hdfs:///user/pig/cleanedData';

--to print table column names
hive> set hive.cli.print.header=true;

--Question1 Query: The top 10 posts by score
SELECT Id, Score, OwnerUserId,Title from posts 
ORDER BY score DESC
LIMIT 10;

--Question2 Query: The top 10 users by post score
SELECT OwnerUserId, SUM(Score) AS Total_Score from posts 
GROUP BY OwnerUserId
ORDER BY Total_Score DESC
LIMIT 10;

--Question3 Query: The number of distinct users, who used the word “Hadoop” in one of their posts
--I am coverting the posts columns to lower case for case insensitivity -- to match all words hadoop or Hadoop  or HADOOP etc
SELECT COUNT(DISTINCT OwnerUserId) AS unique_user_Count from posts 
WHERE (LOWER(BODY) like '%hadoop%' OR LOWER(Title) like '%hadoop%' or LOWER(Tags) like '%hadoop%');
