package graphx

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType
import org.graphframes._
import org.graphframes.lib.AggregateMessages

object Flight {



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


  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder().appName("flightgraphframes").master("local[*]").getOrCreate()

   var file1: String  = "/user/mapr/data/flightdata2018.json"
    var file2: String = "/mapr/demo.mapr.com/data/airports.json"

    if (args.length == 2) {
      file1 = args(0)
      file2 = args(1)

    } else {
      System.out.println("Using hard coded parameters unless you specify the 2 input  files. <file file>   ")
    }
    import spark.implicits._
    val flights= spark.read.format("json").option("inferSchema", "false").schema(schema).load(file1)
  
    flights.createOrReplaceTempView("flights")


    val airports = spark.read.json(file2)
    airports.createOrReplaceTempView("airports")
    airports.show

    val graph = GraphFrame(airports, flights)

    graph.vertices.show
    graph.edges.show

    // What are the longest delays for  flights  that are greater than  1500 miles in  distance?
    println("What are the longest delays for  flights  that are greater than  1500 miles in  distance?")
    graph.edges.filter("dist > 1500").orderBy(desc("depdelay")).show(5)

    // show the longest distance routes
    graph.edges.groupBy("src", "dst")
      .max("dist").sort(desc("max(dist)")).show(4)

    // Which flight routes have the highest average depdelay  ?
    graph.edges.groupBy("src", "dst").avg("depdelay").sort(desc("avg(depdelay)")).show(5)

    //count of departure depdelays by Origin and destination.  
    graph.edges.filter(" depdelay > 40").groupBy("src", "dst").agg(count("depdelay").as("flightcount")).sort(desc("flightcount")).show(5)

    // What are the longest depdelays for flights that are greater than 1500 miles in  distance?
    graph.edges.filter("dist > 1500")
      .orderBy(desc("depdelay")).show(3)

    //What is the average depdelay for depdelayed flights departing from Boston?
    graph.edges.filter("src = 'BOS' and depdelay > 1")
      .groupBy("src", "dst").avg("depdelay").sort(desc("avg(depdelay)")).show

    //which airport has the most incoming flights? The most outgoing ?
    graph.inDegrees.orderBy(desc("inDegree")).show(3)

    graph.outDegrees.orderBy(desc("outDegree")).show(3)

    //What are the highest degree vertexes(most incoming and outgoing flights)?
    graph.degrees.orderBy(desc("degree")).show()

    //What are the 4 most frequent flights in the dataset ? 
    graph.edges.groupBy("src", "dst").count().orderBy(desc("count")).show(4)

    // use pageRank
    val ranks = graph.pageRank.resetProbability(0.15).maxIter(10).run()

    ranks.vertices.orderBy($"pagerank".desc).show()

    val AM = AggregateMessages
    val msgToSrc = AM.edge("depdelay")
    val agg = {
      graph.aggregateMessages
        .sendToSrc(msgToSrc)
        .agg(avg(AM.msg).as("avgdepdelay"))
        .orderBy(desc("avgdepdelay"))
        .limit(5)
    }
    agg.show()
  }
}

