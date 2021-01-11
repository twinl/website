# run program like this: make run DATE=2012090611 SEARCH=test
DATE=2015030305-2015030305
SEARCH=wakker
DEBUGSENT=
HADOOP_HOME=/home/cloud/software/hathi-client/hadoop/share/hadoop
LIBJARS=$(HADOOP_HOME)/common/*:$(HADOOP_HOME)/mapreduce/*:$(HADOOP_HOME)/hdfs/*:$(HADOOP_HOME)/yarn/*:$(HADOOP_HOME)/common/lib/*

all:
	chmod 0 OldFiles
	javac -classpath ".:$(LIBJARS):json-simple-1.1.1.jar:hadoop-mapreduce-client-core-2.7.1.jar" -Xlint:deprecation -Xlint:unchecked -d . *.java
	chmod u+w Search.jar
	jar -cvf Search.jar -C . .
	chmod u-w Search.jar
	chmod 755 OldFiles

run:
	true
#	hadoop jar Search.jar nl.sara.hadoop.Search -libjars Search.jar,json-simple-1.1.1.jar $(DATE) "$(SEARCH)"

sentiment:
	javac -d . Sentiment.java tokenize.java

runsentiment:
	java nl/sara/hadoop/Sentiment $(DEBUGSENT)
