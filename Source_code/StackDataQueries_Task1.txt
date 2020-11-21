
--Query 1 
select top 50000 * from posts where posts.ViewCount > 100000 order by posts.ViewCount DESC

--Query 2
select top 50000 * from posts where posts.ViewCount < 111626 order by posts.ViewCount DESC

--Query 3
select top 50000 * from posts where posts.ViewCount <= 65698 AND id != '2213006' order by posts.ViewCount DESC

--Query 4
select top 50000 * from posts where posts.ViewCount <= 46909 
AND id NOT IN ('41205931' ,'46949311') order by posts.ViewCount DESC

--Query 5: Buffer
select top 50 * FROM posts WHERE posts.ViewCount <= 36495 order by posts.ViewCount DESC


