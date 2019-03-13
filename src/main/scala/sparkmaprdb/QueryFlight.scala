package sparkmaprdb

import org.apache.spark._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import com.mapr.db._
import com.mapr.db.spark._
import com.mapr.db.spark.impl._
import com.mapr.db.spark.sql._
import org.apache.log4j.{ Level, Logger }
//import com.fasterxml.jackson.annotation.{ JsonIgnoreProperties, JsonProperty }

object QueryFlight {


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
    StructField("dist", DoubleType, true),
    StructField("orig_dest", StringType, true),
    StructField("label", DoubleType, true),
    StructField("prediction", DoubleType, true)
  ))

  def main(args: Array[String]) {

    var tableName: String = "/user/mapr/flighttable"
    
    if (args.length == 1) {
      tableName = args(0)
    } else {
      System.out.println("Using hard coded parameters unless you specify the tablename ")
    }
    val spark: SparkSession = SparkSession.builder().appName("querypayment").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    Logger.getLogger("org").setLevel(Level.OFF)

    import spark.implicits._
    // load payment dataset from MapR-DB 
    val df= spark.sparkSession.loadFromMapRDB(tableName, schema)

    println("Flights from MapR-DB")
    df.show

    df.createOrReplaceTempView("flights")
    println("what is the count of predicted delay/notdelay for this dstream dataset")
    
    df.groupBy("orig_dest","label","prediction").count.show
    
    df.groupBy("prediction").count().show()

    println("what is the count of predicted delay/notdelay by scheduled departure hour")
    spark.sql("select crsdephour, prediction, count(prediction) from flights group by crsdephour, prediction order by crsdephour").show
    println("what is the count of predicted delay/notdelay by src")
    spark.sql("select src, prediction, count(prediction) from flights group by src, prediction order by src").show
    println("what is the count of predicted and actual  delay/notdelay by src")
    spark.sql("select src, prediction, count(prediction),label, count(label) from flights group by src, prediction, label order by src, label, prediction").show
    println("what is the count of predicted delay/notdelay by dst")
    spark.sql("select dst, prediction, count(prediction) from flights group by dst, prediction order by dst").show
    println("what is the count of predicted delay/notdelay by src,dst")
    spark.sql("select src,dst, prediction, count(prediction) from flights group by src,dst, prediction order by src,dst").show
    println("what is the count of predicted delay/notdelay by day of the week")
    spark.sql("select dofW, prediction, count(prediction) from flights group by dofW, prediction order by dofW").show
    println("what is the count of predicted delay/notdelay by carrier")
    spark.sql("select carrier, prediction, count(prediction) from flights group by carrier, prediction order by carrier").show

    val lp = df.select("label", "prediction")
    val counttotal = df.count()


    val correct = lp.filter($"label" === $"prediction").count()
    val wrong = lp.filter(not($"label" === $"prediction")).count()
    val ratioWrong = wrong.toDouble / counttotal.toDouble
    val ratioCorrect = correct.toDouble / counttotal.toDouble
    val truep = lp.filter($"prediction" === 1.0)
      .filter($"label" === $"prediction").count() / counttotal.toDouble
    val truen = lp.filter($"prediction" === 0.0)
      .filter($"label" === $"prediction").count() / counttotal.toDouble
    val falsep = lp.filter($"prediction" === 1.0)
      .filter(not($"label" === $"prediction")).count() / counttotal.toDouble
    val falsen = lp.filter($"prediction" === 0.0)
      .filter(not($"label" === $"prediction")).count() / counttotal.toDouble

      println("counttotal ", counttotal)
    println("ratio correct ", ratioCorrect)
    println("ratio wrong ", ratioWrong)
    println("correct ", correct)
    println("true positive ", truep)
    println("true negative ", truen)
    println("false positive ", falsep)
    println("false negative ", falsen)
  }
}

