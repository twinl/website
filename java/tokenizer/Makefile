# run program like this: make run MYPATH=twitter/2012/10/14
MYPATH=twitter/2015/03/02
HADOOP_HOME=/home/cloud/software/hathi-client/hadoop-2.6.0/share/hadoop
LIBJARS=$(HADOOP_HOME)/common/*:$(HADOOP_HOME)/mapreduce/*:$(HADOOP_HOME)/hdfs/*:$(HADOOP_HOME)/yarn/*:$(HADOOP_HOME)/common/lib/*

all:
	chmod 0 OldFiles
	javac -classpath ".:$(LIBJARS):json-simple-1.1.1.jar" -Xlint:deprecation -Xlint:unchecked -d . *.java
	jar -cvf Tokenizer.jar -C . .
	chmod 755 OldFiles

run:
	true
#	hadoop jar Tokenizer.jar nl.sara.hadoop.Tokenizer -libjars json-simple-1.1.1.jar,Tokenizer.jar $(MYPATH)
