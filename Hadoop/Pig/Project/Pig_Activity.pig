inputFile1 = LOAD 'hdfs:///user/root/ravi/pig_project/episodeIV_dialogues.txt' USING PigStorage('\t') AS (Name:chararray,Dialogue:chararray);
inputFile2 = LOAD 'hdfs:///user/root/ravi/pig_project/episodeVI_dialogues.txt' USING PigStorage('\t') AS (Name:chararray,Dialogue:chararray);
inputFile3 = LOAD 'hdfs:///user/root/ravi/pig_project/episodeV_dialogues.txt' USING PigStorage('\t') AS (Name:chararray,Dialogue:chararray);
files_consolidated= Union inputFile1,inputFile2,inputFile3;
GroupByName = GROUP files_consolidated BY Name;
CountByName = FOREACH GroupByName GENERATE $0 AS NAME,COUNT($1) AS No_Of_Lines;
orderbydesc= order CountByName by No_Of_Lines desc;
STORE orderbydesc INTO 'hdfs:///user/root/ravi/pig_project/DialogueOutput_updated' USING PigStorage('\t');