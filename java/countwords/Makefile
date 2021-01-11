# run program like this: make run MYPATH=cache/2012/10/14/baumgartner
MYPATH=cache/2015/03/03/wakker/Tokenizer
HADOOP_HOME=/home/cloud/software/hathi-client/hadoop-2.6.0/share/hadoop
LIBJARS=$(HADOOP_HOME)/common/*:$(HADOOP_HOME)/mapreduce/*:$(HADOOP_HOME)/hdfs/*:$(HADOOP_HOME)/yarn/*:$(HADOOP_HOME)/common/lib/*

all:
	chmod 0 OldFiles
	javac -classpath ".:$(LIBJARS)" -Xlint:deprecation -Xlint:unchecked -d . *.java
	jar -cvf CountWords.jar -C . .
	chmod 755 OldFiles

run:
	true
#	hadoop jar CountWords.jar nl.sara.hadoop.CountWords -libjars CountWords.jar $(MYPATH)
