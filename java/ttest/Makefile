# run program like this: make run MYPATH1=cache/2012/10/14/baumgartner/CountWords MYPATH2=annotations/2012/10/14/CountWords
MYPATH1=cache/2015030305-2015030305/Search/wakker/CountWords/part-r-00000.gz
MYPATH2=cache/2015/03/03/Tokenizer/CountWords/part-r-00000.gz
TYPESQUERY=18721
TOKENSQUERY=127160
TYPESREFERENCE=1074395
TOKENSREFERENCE=33904668
SUBTRACT=yes
HADOOP_HOME=/home/cloud/software/hathi-client/hadoop-2.6.0/share/hadoop
LIBJARS=$(HADOOP_HOME)/common/*:$(HADOOP_HOME)/mapreduce/*:$(HADOOP_HOME)/hdfs/*:$(HADOOP_HOME)/yarn/*:$(HADOOP_HOME)/common/lib/*

all:
	chmod 0 OldFiles
	javac -classpath ".:$(LIBJARS)" -Xlint:deprecation -Xlint:unchecked -d . *.java
	jar -cvf TTest.jar -C . .

run:
	true
#	hadoop jar TTest.jar nl.sara.hadoop.TTest -libjars TTest.jar $(MYPATH1) $(MYPATH2) $(TYPESQUERY) $(TOKENSQUERY) $(TYPESREFERENCE) $(TOKENSREFERENCE) $(SUBTRACT)
