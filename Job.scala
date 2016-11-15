package com.sparkProject

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Job {

  def main(args: Array[String]): Unit = {

    // SparkSession configuration
    val spark = SparkSession
      .builder
      .appName("spark session TP_parisTech")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    /********************************************************************************
      *
      *        TP 2 : début du projet
      *
      ********************************************************************************/

    // Create here a DataSet from CSV file
    val df = spark.read.option("header", "true").
        option("inferSchema", "true").option("comment", "#").csv("/Users/rom/Documents/tp_spark2/cumulative.csv")

    // Display file information.

    println("\r\n------ File information ------")
    println("Number of columns: ", df.columns.length)
    println("Number of rows: ", df.count)

    // Display 10 to 20 columns
    // df.columns returns an Array. In scala arrays have a method “slice” returning a slice of the array
    println("\r\n------ Few columns (10 to 20) ------")
    val col_10_to_20 = df.columns.slice(10, 20)
    println(df.select(col_10_to_20.map(col): _*).show(5))

    // DF show
    println("\r\n------ Schema ------ [df.printSchema()]")
    print(df.printSchema())

    // Display koi_disposition
    println("\r\n------ koi_disposition (count distinct) ------ [df.groupBy(\"koi_disposition\").count().show(5)]")
    println(df.groupBy("koi_disposition").count().show(5))

    // Keep only CONFIRMED and CANDIDATE
    println("\r\n------ Create DF2, that will contain only CONFIRMED or CANDIDATE ------ [df.filter()]")
    val df2 = df.filter(
      df("koi_disposition").like("CONFIRMED")
        or df("koi_disposition").like("CANDIDATE"))

    // Display koi_eccen_err1
    println("\r\n------ Display DF grouped by koi_eccen_err1 ------ [df2.groupBy()]")
    println(df2.groupBy("koi_eccen_err1").count().show(5))

    // Remove koi_eccen_err1

    println("\r\n------ Drop koi_eccen_err1 ------ [df2.drop()]")
    val df3 = df2.drop("koi_eccen_err1")

    // Drop many columns
    val  drop_col = Array(
      "kepid",
      "koi_fpflag_nt",
      "koi_fpflag_ss",
      "koi_fpflag_co",
      "koi_fpflag_ec",
      "koi_sparprov",
      "koi_trans_mod",
      "koi_datalink_dvr",
      "koi_datalink_dvs",
      "koi_tce_delivname",
      "koi_parm_prov",
      "koi_limbdark_mod",
      "koi_fittype",
      "koi_disp_prov",
      "koi_comment",
      "kepoi_name",
      "kepler_name",
      "koi_vet_date",
      "koi_pdisposition")

    println("\r\n------ Drop many columns ------ [df3.drop()]")
    // map() va appeler col()
    println(df3.select(drop_col.map(col): _*).show(5))

    val df4 = df3.drop(drop_col:_*)

    // D’autres colonnes ne contiennent qu’une seule valeur (null, une string, un entier, ou autre).
    var drop_col2V1 = List[String]()

    // Mettre en cache DF4, il ne sera pas évalué systématiquement
    df4.persist()

    println("\r\n------ Drop column where count <= 1 (method 1)------")
    for(col_id <- df4.columns) {
      if (df4.groupBy(col_id).count().count() <= 1) {
        drop_col2V1 ++= List(col_id)
      }
    }

    println(drop_col2V1)

    println("\r\n------ Drop column where count <= 1 (method 2)------")
    val drop_col2V2 = df4.columns.filter{
      case (column:String) =>
      df4.agg(countDistinct(column)).first().getLong(0) <= 1 }

    println(drop_col2V2)

    val df5 = df4.drop(drop_col2V1:_*)

    val df5_stats = df5.describe()

    val df5_5columns = df5_stats.columns.slice(0,5)

    df5_stats.select(df5_5columns.map(col):_*).show()

    val df6 = df5.na.fill(0.0)

    val df6_labels = df6.select("rowid", "koi_disposition")
    val df6_features = df6.drop("koi_disposition")

    val df7 = df6_features.join(df6_labels, usingColumn = "rowid")


    def udf_sum = udf((col1: Double, col2: Double) => col1 + col2)
    def udf_dec = udf((col1: Double, col2: Double) => col1 - col2)

    // koi_ror_err2 is NEGATIVE
    val df8 = df7.withColumn("koi_ror_max", udf_dec($"koi_ror", $"koi_ror_err2")).withColumn("koi_ror_min", udf_sum($"koi_ror", $"koi_ror_err2"))

    df8
      .coalesce(1) // optional : regroup all data in ONE partition, so that results are printed in ONE file
      // >>>> You should not that in general, only when the data are small enough to fit in the memory of a single machine.
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("/Users/rom/Documents/tp_spark2/cleanedDataFrame.csv")




  }


}
