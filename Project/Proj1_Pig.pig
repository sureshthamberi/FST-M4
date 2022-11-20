-- Load the input file
episode = LOAD 'hdfs:///user/suresh/inputs/' USING PigStorage('\t') AS (character:chararray,dialogue:chararray);
--Skip the Header row during mapreduce job
ranked = rank episode;
headerSkippedepisode = FILTER ranked BY (rank_episode > 2 );
-- Group data using the character column
GroupBycharacter = GROUP headerSkippedepisode BY character;
-- Count the occurence of each character (Reduce) and Generate result format
Accrances = FOREACH GroupBycharacter GENERATE $0 AS name, COUNT($1) AS no_of_lines;
-- Arraning them in Order in report
characterAccrces = ORDER Accrances BY no_of_lines DESC;
-- Save result in HDFS folder
STORE characterAccrces INTO 'hdfs:///user/suresh/proj1Reslt_whole';

