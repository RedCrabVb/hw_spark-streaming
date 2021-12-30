import org.apache.spark.sql.functions.{avg, max, col, from_json}
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats


// https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
object SparkApp extends App {
  implicit val formats = DefaultFormats

  val schema = new StructType()
    .add("id", IntegerType)
    .add("amount", IntegerType)
    .add("shoppingList", StringType)

  val spark = SparkSession.builder
    .appName("spark-streaming-hw")
    .master(sys.env.getOrElse("spark.master", "local[*]"))
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val brokers = "localhost:9092"
  val topic = "records"

  val records: DataFrame = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("subscribe", topic)
    .load()

  val query = records
    .withColumn("result", from_json(col("value").cast(StringType), schema).getItem("amount"))
    .select(avg("result").as("Average amount price"), max("result").as("Max average amount"))
    .writeStream
    .outputMode("update")
    .trigger(ProcessingTime("10 seconds"))
    .format("console")
    .start()


  query.awaitTermination()
}
