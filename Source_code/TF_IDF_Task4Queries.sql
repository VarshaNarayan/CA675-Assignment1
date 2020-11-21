--Data Preparation for Task 4
  
--Creating and inserting the Query2 results to temp table 
CREATE TABLE TopUsersScore AS SELECT OwnerUserId, SUM(Score) AS Total_Score from posts 
GROUP BY OwnerUserId
ORDER BY Total_Score DESC
LIMIT 10;

--Create another table for storing the text contents of these top users 
CREATE TABLE TopUsersPosts AS
SELECT OwnerUserId,Body,Title,Tags from posts 
WHERE OwnerUserId IN (SELECT OwnerUserId from TopUsersScore)
GROUP BY OwnerUserId, Body, Title, Tags;

--COPY Table to HDFS
INSERT OVERWRITE DIRECTORY '/user/hive/hiveTableContent'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM TopUsersPosts;

=====================================================

--Processing HIVE TABLE -hiveScriptPig
-- hdfs dfs -copyToLocal /user/hive/hiveClean3 /home/varsha_narayan9/

====================================================
--TF_IDF calculation

hive> add jar hivemall-core-0.4.2-rc.2-with-dependencies.jar;
hive> source define-all.hive;

hive> create temporary macro max2(x INT, y INT) if(x>y,x,y);

hive> create temporary macro tfidf(tf FLOAT, df_t INT, n_docs INT) tf * (log(10, CAST(n_docs as FLOAT)/max2(1,df_t)) + 1.0);

create external table TF_IDFCalc1 (
  userid int,
  posts string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/varsha_narayan9/hiveClean3/part-m-00000' INTO TABLE TF_IDFCalc1;

SELECT * FROM TF_IDFCalc1;

create or replace view posts_exploded
as
select
  userid, 
  word
from
  TF_IDFCalc1 LATERAL VIEW explode(tokenize(posts,true)) t as word
where
  not is_stopword(word);
  
create or replace view term_frequency 
as
select
  userid, 
  word,
  freq
from (
select
  userid,
  tf(word) as word2freq
from
  posts_exploded
group by
  userid
) t 
LATERAL VIEW explode(word2freq) t2 as word, freq;

create or replace view document_frequency
as
select
  word, 
  count(distinct userid) docs
from
  posts_exploded
group by
  word;

select count(distinct userid) from TF_IDFCalc1;

set hivevar:n_docs=10;

create or replace view tfidf
as
select
  tf.userid,
  tf.word, 
  -- tf.freq * (log(10, CAST(${n_docs} as FLOAT)/max2(1,df.docs)) + 1.0) as tfidf
  tfidf(tf.freq, df.docs, ${n_docs}) as tfidf
from
  term_frequency tf 
  JOIN document_frequency df ON (tf.word = df.word)
Order BY tf.userid;


INSERT OVERWRITE DIRECTORY '/user/hive/TF_IDF_FinalResult'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT S.userid, S.word, S.tfidf
FROM
 (SELECT userid, word, tfidf,  row_number() over (partition by userid) as r FROM tfidf) S
WHERE S.r < 11;




