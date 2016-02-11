import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

object SparkSqlContextTempTableIdentifier {

  private def identifierCheck(df: DataFrame, identifier: String): Unit = {
    df.registerTempTable(identifier)
    df.sqlContext.dropTempTable(identifier)
  }

  def main(args: Array[String]): Unit = {
    println("Initializing Spark")
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("SparkSqlContextTempTableIdentifier")
      .set("spark.ui.enabled", "false")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val rows = List(Row("foo"), Row("bar"))
    val schema = StructType(Seq(StructField("co`l", StringType)))
    val rdd = sc.parallelize(rows)
    val df = sqlContext.createDataFrame(rdd, schema)

    val selectedDf = df.selectExpr("`co``l`")		// Exception will be thrown here

    println("Print selectedDf content:")
    selectedDf.collect().foreach(println(_))
  }
}
