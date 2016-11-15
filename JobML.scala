package com.sparkProject
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.log4j.Logger
import org.apache.log4j.Level


/**
  * Created by rom on 25/10/16.
  */
object JobML {

  def main(args: Array[String]): Unit = {

    /*
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF) */

    if (args.length == 0) {
      println("""Usage tp_spark-assembly-1.0.jar file where file is a .parquet file. Example: tp_spark-assembly-1.0.jar /Users/rom/Documents/tp_spark2/cleanedDataFrame.parquet""")
    }
    else
    {

      // SparkSession configuration
      val spark = SparkSession
        .builder
        .appName("Spark Session TP 4-5")
        .getOrCreate()

      // SparkSession configuration
      // val spark = SparkSession.builder.master("spark://romains-macbook-pro.home:7077").appName("Spark Session TP 4-5").getOrCreate()

      // Download clean data
      val df = spark.read.parquet(args(0))

      println("r\nUsing file: ", args(0))
      println("r\nScaling features, and defining label.")

      // Select column that will be compacted in a vector and scaled
      val x_colnames_to_scale = df.columns.filter(_ != "koi_disposition").filter(_ != "rowid")

      // Define vector assemble that will transform x_colnames_to_scale in one vector
      val assembler = new VectorAssembler().setInputCols(x_colnames_to_scale).setOutputCol("features0")

      // Select columns we want to keep.
      val col_to_keep = Seq("features0", "rowid", "koi_disposition")


      // Transform ANS keep only "col_to_keep" columns
      val df2 = assembler.transform(df).select(col_to_keep.head, col_to_keep.tail: _*)

      // Scale in a new column "scaledFeatures"
      val scaler = new StandardScaler().setInputCol("features0").setOutputCol("features").setWithStd(true).setWithMean(false)

      // Compute summary statistics by fitting the StandardScaler.
      val scalerModel = scaler.fit(df2)

      // Normalize each feature to have unit standard deviation.
      val df4 = scalerModel.transform(df2)

      // Now we want to index CONFIRMED an FALSE POSITIVE
      val indexer = new StringIndexer().setInputCol("koi_disposition").setOutputCol("label").fit(df4)
      val df5 = indexer.transform(df4)

      // Split the data into training and test sets (30% held out for testing).
      println("Split the data into training and test sets (30% held out for testing)")
      val Array(trainingData, testData) = df5.randomSplit(Array(0.7, 0.3))


      println("Train data (5 first lines):")
      println(trainingData.show(5))
      println("Test data (5 first lines):")
      println(testData.show(5))
      println("Full dataset (5 first lines):")
      println(df5.show(5))


      // Prepare LR for cross validation
      val lrTest = new LogisticRegression()
        .setFeaturesCol("features")
        .setLabelCol("label")
        .setStandardization(true)
        .setFitIntercept(true)
        .setTol(1.0e-5)
        .setMaxIter(300)

      val elasticRange = Array(0.1, 0.5, 0.9, 1.0)
      val paramRange = Array(0.000001, 0.00001, 0.0001, 0.001, 0.01, 0.1, 1)

  /*    val elasticRange = Array(0.1,  1.0)
      val paramRange = Array( 0.1, 1) */

      println("Cross validation will start and will take a long time.")
      println("We will test for ElasticNet", elasticRange.mkString(" "))
      println("We will test for Logistic Parameters", paramRange.mkString(" "))

      //val paramGrid = new ParamGridBuilder().addGrid(lrTest.elasticNetParam, Array(0.1, 0.5, 1.0)).addGrid(lrTest.regParam, Array(0.000001, 0.00001, 0.0001, 0.001, 0.01, 0.1, 1)).build()
      val paramGrid = new ParamGridBuilder()
        .addGrid(lrTest.elasticNetParam, elasticRange)
        .addGrid(lrTest.regParam, paramRange)
        .build()

      val cv = new CrossValidator()
        .setEstimator(lrTest)
        .setEvaluator(new BinaryClassificationEvaluator)
        .setEstimatorParamMaps(paramGrid).setNumFolds(3)

      val cvModel = cv.fit(trainingData)

      val results = cvModel.transform(testData.toDF).select("label", "prediction").collect
      val numCorrectPredictions = results.map(row => if (row.getDouble(0) == row.getDouble(1)) 1 else 0).foldLeft(0)(_ + _)
      val accuracy = 1.0D * numCorrectPredictions / results.size



      println("Test set accuracy: %.3f".format(accuracy))
      println("Best model parameters: ")
      println(cvModel.getEstimatorParamMaps.zip(cvModel.avgMetrics).maxBy(_._2)._1)
    }


  }

}
