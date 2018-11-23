1. Hadoop java file 작성
2. jar 파일 생성
$ mkdir wordcount_classes
$ javac -classpath $HADOOP_CLASSPATH -d wordcount_classes WordCount.java
$ jar -cvf WordCount.jar -C wordcount_classes/ .
3. jar 파일 실행
$ hadoop jar WordCount.jar WordCount /user/wordcount/input /user/wordcount/output
input is input directory in hdfs and output is result directory in hdfs.