package stream

// http://maprdocs.mapr.com/home/Spark/Spark_IntegrateMapRStreams.html

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.streaming._

import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.tuning._

import com.mapr.db.spark.impl._
import com.mapr.db.spark.streaming._
import com.mapr.db.spark.sql._
import com.mapr.db.spark.streaming.MapRDBSourceConfig

/**
 * Consumes messages from a topic in MapR Streams using the Kafka interface,
 * enriches the message with  the k-means model cluster id and publishs the result in json format
 * to another topic
 * Usage: SparkKafkaConsumerProducer  <model> <topicssubscribe> <topicspublish>
 *
 *   <model>  is the path to the saved model
 *   <topic> is a  topic to consume from
 *   <tableName> is a table to write to
 * Example:
 *    $  spark-submit --class com.sparkkafka.uber.SparkKafkaConsumerProducer --master local[2] \
 * mapr-sparkml-streaming-uber-1.0.jar /user/user01/data/savemodel  /user/user01/stream:ubers /user/user01/stream:uberp
 *
 *    for more information
 *    http://maprdocs.mapr.com/home/Spark/Spark_IntegrateMapRStreams_Consume.html
 */

object StructuredStreamingConsumer extends Serializable {

  val schema = StructType(Array(
    StructField("id", StringType, true),
    StructField("fldate", StringType, true),
    StructField("month", IntegerType, true),
    StructField("dofW", IntegerType, true),
    StructField("carrier", StringType, true),
    StructField("src", StringType, true),
    StructField("dst", StringType, true),
    StructField("crsdephour", IntegerType, true),
    StructField("crsdeptime", IntegerType, true),
    StructField("depdelay", DoubleType, true),
    StructField("crsarrtime", IntegerType, true),
    StructField("arrdelay", DoubleType, true),
    StructField("crselapsedtime", DoubleType, true),
    StructField("dist", DoubleType, true)
  ))

  def main(args: Array[String]): Unit = {

    // MapR Event Store for Kafka Topic to read from 
    var topic: String = "/user/mapr/stream:flights"
    // MapR Database table to write to 
    var tableName: String = "/user/mapr/flighttable"
    // Directory to read the saved ML model from 
    var modeldirectory: String = "/user/mapr/model/"

    if (args.length == 3) {
      topic = args(0)
      modeldirectory = args(1)
      tableName = args(2)

    } else {
      System.out.println("Using hard coded parameters unless you specify topic model directory and table. <topic model table>   ")
    }

    val spark: SparkSession = SparkSession.builder().appName("stream").master("local[*]").getOrCreate()

    import spark.implicits._
    val model = org.apache.spark.ml.PipelineModel.load(modeldirectory)

    // Print out the model feature importances
    val featureCols = Array("carrierIndexed", "dstIndexed", "srcIndexed", "dofWIndexed", "orig_destIndexed", "crsdephour", "crsdeptime", "crsarrtime", "crselapsedtime", "dist")
    val rfm = model.stages.last.asInstanceOf[RandomForestClassificationModel]
    val featureImportances = rfm.featureImportances
    featureCols.zip(featureImportances.toArray).sortBy(-_._2).foreach { case (feat, imp) => println(s"feature: $feat, importance: $imp") }

    val df1 = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "maprdemo:9092")
      .option("subscribe", topic)
      .option("group.id", "testgroup")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", false)
      .option("maxOffsetsPerTrigger", 1000)
      .load()

    println(df1.printSchema)

    println("Enrich Transformm Stream")

    // cast the df1 column value to string
    // use the from_json function to convert value JSON string to flight schema
    val df2 = df1.select($"value" cast "string" as "json").select(from_json($"json", schema) as "data").select("data.*")

    // add column orig_dest, needed feature for ML model
    val df3 = df2.withColumn("orig_dest", concat($"src", lit("_"), $"dst"))

    // transform the DataFrame with the model pipeline, which will tranform the features according to the pipeline, 
    // estimate and then return the predictions in a column of a new DateFrame
    val predictions = model.transform(df3)

    // select the columns that we want to store
    val df4 = predictions.drop("features").drop("rawPrediction").drop("probability")
    println(df4.printSchema)

    println("write stream")

    import com.mapr.db.spark.impl._
    import com.mapr.db.spark.streaming._
    import com.mapr.db.spark.sql._
    import com.mapr.db.spark.streaming.MapRDBSourceConfig
    val writedb = df4.writeStream
      .format(MapRDBSourceConfig.Format)
      .option(MapRDBSourceConfig.TablePathOption, tableName)
      .option(MapRDBSourceConfig.IdFieldPathOption, "id")
      .option(MapRDBSourceConfig.CreateTableOption, false)
      .option("checkpointLocation", "/tmp/flight")
      .option(MapRDBSourceConfig.BulkModeOption, true)
      .option(MapRDBSourceConfig.SampleSizeOption, 1000)
      .start()

    writedb.awaitTermination(300000)

  }

}