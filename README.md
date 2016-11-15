# SparkInTheStars
A Scala / Spark 2.0 project to predict if expolanets are in habitable zone of their star.

## Usage
tp_spark-assembly-1.0.jar file where file is a .parquet file. 
Example: 
```
tp_spark-assembly-1.0.jar /Users/johndoe/Documents/tp_spark2/cleanedDataFrame.parquet
```
## Command line to launch the project
```
./spark-submit --conf spark.eventLog.enabled=true --conf spark.eventLog.dir="/tmp" --driver-memory 3G --executor-memory 4G --class com.sparkProject.JobML /Users/johndoe/Documents/tp_spark2/target/scala-2.11/tp_spark-assembly-1.0.jar /Users/johndoe/Documents/tp_spark2/cleanedDataFrame.parquet
```
where:

* _/Users/johndoe/Documents/tp_spark2/target/scala-2.11/tp_spark-assembly-1.0.jar_ needs to be replaced by your own JAR.
* _/Users/johndoe/Documents/tp_spark2/cleanedDataFrame.parquet_ needs to be replace by your own file.

## ToDo / Improvement
1. Train the best model with all the test features.
2. Save the the best model.
3. Clean display, sometime '(' ')' appear.
4. Display progress of the cross validation.

## Execution Result

__Cross validation can take more than 1 hour !__

```
Processing file: ,/Users/rom/Documents/tp_spark2/cleanedDataFrame.parquet)

Scaling features, and defining label.
Split the data into training and test sets (30% held out for testing)           
Train data (5 first lines):
+--------------------+-----+---------------+--------------------+-----+
|           features0|rowid|koi_disposition|            features|label|
+--------------------+-----+---------------+--------------------+-----+
|(111,[0,1,2,3,4,5...|  323| FALSE POSITIVE|(111,[0,1,2,3,4,5...|  0.0|
|(111,[0,1,2,3,4,5...| 2906| FALSE POSITIVE|(111,[0,1,2,3,4,5...|  0.0|
|(111,[0,1,2,3,4,5...|  720| FALSE POSITIVE|(111,[0,1,2,3,4,5...|  0.0|
|(111,[0,1,2,3,4,5...|  366| FALSE POSITIVE|(111,[0,1,2,3,4,5...|  0.0|
|(111,[0,1,2,3,4,5...|  504| FALSE POSITIVE|(111,[0,1,2,3,4,5...|  0.0|
+--------------------+-----+---------------+--------------------+-----+
only showing top 5 rows

Test data (5 first lines):
+--------------------+-----+---------------+--------------------+-----+
|           features0|rowid|koi_disposition|            features|label|
+--------------------+-----+---------------+--------------------+-----+
|(111,[0,1,2,3,4,5...|  514| FALSE POSITIVE|(111,[0,1,2,3,4,5...|  0.0|
|(111,[0,1,2,3,4,5...| 3078|      CONFIRMED|(111,[0,1,2,3,4,5...|  1.0|
|(111,[0,1,2,3,4,5...| 2931| FALSE POSITIVE|(111,[0,1,2,3,4,5...|  0.0|
|(111,[0,1,2,3,4,5...|  454| FALSE POSITIVE|(111,[0,1,2,3,4,5...|  0.0|
|(111,[0,1,2,3,4,5...|  328| FALSE POSITIVE|(111,[0,1,2,3,4,5...|  0.0|
+--------------------+-----+---------------+--------------------+-----+
only showing top 5 rows

Full dataset (5 first lines):
+--------------------+-----+---------------+--------------------+-----+
|           features0|rowid|koi_disposition|            features|label|
+--------------------+-----+---------------+--------------------+-----+
|[9.48803146,2.95E...|    1|      CONFIRMED|[0.08483524146264...|  1.0|
|[54.418464,2.686E...|    2|      CONFIRMED|[0.48657127170465...|  1.0|
|[2.525593315,3.66...|    5|      CONFIRMED|[0.02258206242440...|  1.0|
|[11.09431923,2.13...|    6|      CONFIRMED|[0.09919752634764...|  1.0|
|[4.13443005,1.061...|    7|      CONFIRMED|[0.03696713834485...|  1.0|
+--------------------+-----+---------------+--------------------+-----+
only showing top 5 rows

Cross validation will start and will take a long time.
(We will test for ElasticNet,0.1 0.5 0.9 1.0)
(We will test for Logistic Parameters,1.0E-6 1.0E-5 1.0E-4 0.001 0.01 0.1 1.0)
```
