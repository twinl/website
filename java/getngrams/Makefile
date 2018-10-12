# run program like this: make run DATE=2012090611 SEARCH=test
DATE=2015030305-2015030305
HADOOP_HOME=/home/cloud/software/hathi-client/hadoop/share/hadoop
LIBJARS=$(HADOOP_HOME)/common/*:$(HADOOP_HOME)/mapreduce/*:$(HADOOP_HOME)/hdfs/*:$(HADOOP_HOME)/yarn/*:$(HADOOP_HOME)/common/lib/*

all:
	chmod 0 OldFiles
	javac -classpath ".:$(LIBJARS):json-simple-1.1.1.jar" -Xlint:deprecation -Xlint:unchecked -d . *.java
	jar -cvf GetNgrams.jar -C . .
	chmod 755 OldFiles

run:
	hadoop jar GetNgrams.jar nl.sara.hadoop.GetNgrams -libjars GetNgrams.jar,json-simple-1.1.1.jar $(DATE)

sentiment:
	javac -d . Sentiment.java tokenize.java

runsentiment:
	java nl/sara/hadoop/Sentiment $(DEBUGSENT)
