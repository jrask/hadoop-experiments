mvn compile && rm -rf count && hadoop com.jayway.hadoop.fress.CountMessageIdsDriver -conf conf/hadoop-local.xml excluded/part-r-00000 count
