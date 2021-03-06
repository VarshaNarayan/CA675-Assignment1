csvFile = LOAD '/tmp/processed_data.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') AS (Id:int, PostTypeId:int,  AcceptedAnswerId:int, ParentId:int, CreationDate:chararray, DeletionDate:chararray, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:chararray, LastActivityDate:chararray, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:chararray, CommunityOwnedDate:chararray);

requiredFieldsData = FOREACH csvFile GENERATE $0 AS Id:int, $6 AS Score:int, REPLACE($8,'\\n|\\r|<br>|\\t|<.+?>',' ') AS Body:chararray, $9 AS OwnerUserId:int, $15 AS Title:chararray, $16 AS Tags:chararray;

finalData = FILTER requiredFieldsData BY (OwnerUserId IS NOT NULL)

STORE finalData INTO '/user/pig/cleanedData' USING PigStorage(',');
