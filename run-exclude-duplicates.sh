mvn compile && rm -rf output && hadoop com.jayway.hadoop.fress.ExtractJsonRpcMessagesDriver -conf conf/hadoop-local.xml input/fress output
mvn compile && rm -rf excluded && hadoop com.jayway.hadoop.fress.ExcludeDuplicateMessagesDriver -conf conf/hadoop-local.xml output/part-r-00000 excluded
