wiki-spark-corenlp
==================

This is an experimental prototype for fast NLP preprocessing of large English text collections like Wikipedia.
It uses the [Standford CoreNlp library](https://stanfordnlp.github.io/CoreNLP/) (Version 3.9.1) for NLP preprocessing, i.e. sentence splitting, tokenization, lemmatization and part-of-speech-tagging.
[Apache Spark](http://spark.apache.org/) (Version 2.3.0) is used to partition the set of documents and process each part in parallel.
Expensive recreation of corenlp pipeline objects is avoided by using [Apache Commons Pool](https://commons.apache.org/proper/commons-pool/) that organizes the borrowing and reuses of the pipeline objects by the Spark worker threads.
This methods works only, if Spark is run in local mode on a single machine.

How to prepare the input?
-----------
Extract the texts from a Wikipedia dump in a csv file using the (extended) [Wikiforia](https://github.com/hinneburg/wikiforia) project.
The produced csv file has the schema 	
```
page_id, title, revision, type, ns_id, ns_name, text
```
Filter for pages with name space id (`ns_id`) equals 0 as only those contain the articles.

How to build and use?
-----------
Install Java JDK (>= 1.7)  and Scala 2.11, which is needed for Spark.
The code is tested with open-jdk on Ubuntu Linux 14.04.
Download this project and build it with
```
mvn clean package
```
This will fetch all java libraries and models, which in case of Stanford Corenlp are quite large, e.g. 350MB for models.

The code is best run using maven as all libraries and models are put in the class path.
The JVM parameters are set by
```
export MAVEN_OPTS="-Xmx< main memory for preprocessing with Spark and corenlp >"
# example
export MAVEN_OPTS="-Xmx80G"
```
The implementation is optimized for speed and uses therefore as much main memory as possible.
First, the full corpus is partitioned and cached in memory to avoid that reading input and writing output to same disk is interleaved. This should save time consuming disk seek operations.
Second, a CoreNLP-pipeline is applied to each document that produces a list of tokens with annotations.
This is done in parallel for each partition using Sparks [flatMap function](http://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/Dataset.html#flatMap-org.apache.spark.api.java.function.FlatMapFunction-org.apache.spark.sql.Encoder-).

For starting the NLP preprocessing, four parameters are needed:
  - path/to/data/file
  - path/to/output/file
  - number of local parallel worker threads
  - partition factor

The number of local parallel workers should be set to number of cores on the machine.
The number of partitions for the dataset is number of worker * partition factor.
Spark recommends to use at least twice as much partitions as there are number of workers. Thus, the partition factor should be at least 2. Larger partition factors produce a little more overhead with the benefit of better balancing the data in case of skewed partitions.

```
mvn exec:java \
   -Dexec.mainClass="cc.topicexplorer.wikipedia.spark.corenlp.WikiSparkCorenlp" \
	 -Dexec.args="<path/to/data/file> <path/to/output/file> <number of local parallel worker threads> <partition factor>"
```
Test Run on the English Wikipedia
-----------
The output is a directory with csv files that look like
```
beginPosition,endPosition,lemma,pageId,pos,sentenceNo,token,tokenNo
0,5,these,21146766,DT,0,These,0
6,9,be,21146766,VBP,0,are,1
10,13,the,21146766,DT,0,the,2
14,22,official,21146766,JJ,0,official,3
23,30,result,21146766,NNS,0,results,4
31,33,of,21146766,IN,0,of,5
34,37,the,21146766,DT,0,the,6
38,41,men,21146766,NN,0,Men,7
41,43,'s,21146766,POS,0,'s,8
44,49,"1,500",21146766,CD,0,"1,500",9
50,56,metre,21146766,NNS,0,metres,10
57,62,event,21146766,NN,0,event,11
63,65,at,21146766,IN,0,at,12
66,69,the,21146766,DT,0,the,13
70,74,1978,21146766,CD,0,1978,14
75,83,european,21146766,JJ,0,European,15
84,97,championship,21146766,NNS,0,Championships,16
98,100,in,21146766,IN,0,in,17
101,107,Prague,21146766,NNP,0,Prague,18
107,108,",",21146766,",",0,",",19
109,123,Czechoslovakia,21146766,NNP,0,Czechoslovakia,20
123,124,.,21146766,.,0,.,21
125,128,the,21146766,DT,1,The,0
129,134,final,21146766,JJ,1,final,1
135,138,be,21146766,VBD,1,was,2
139,143,hold,21146766,VBN,1,held,3
...
```
The processing of the English wikipedia (dump from 2018-02-20) with 5.587.573 articles (about 14GB) took 4 hours 32 minutes on a machine with 32 cores and 80 GB main memory. All CPUs were constantly in use during the processing. The final number of tokens is 2.757.360.284, the average processing speed is 168.956 tokens per second and the total size of the output csv files is 98 GB.
