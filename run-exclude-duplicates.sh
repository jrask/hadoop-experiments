mvn compile && rm -rf excluded && hadoop com.jayway.hadoop.fress.ExcludeDuplicateMessagesDriver -conf conf/hadoop-local.xml output/part-r-00000 excluded
