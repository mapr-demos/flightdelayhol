package machinelearning

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.tuning._
import org.apache.spark.sql.functions.{ concat, lit }

object Flight {

  case class Flight(id: String, 
                    fldate: String, month: Integer, dofW: Integer, carrier: String, 
                    src: String, dst: String, crsdephour: Integer, crsdeptime: Integer, 
                    depdelay: Double, crsarrtime: Integer, arrdelay: Double, crselapsedtime: 
                    Double, dist: Double)

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

    val spark: SparkSession = SparkSession.builder().appName("flightdelay").master("local[*]").getOrCreate()

    var file: String = "/user/mapr/data/flightdata2018.json"

    var modeldirectory: String = "/user/mapr/model"

    if (args.length == 1) {
      file = args(0)

    } else {
      System.out.println("Using hard coded parameters unless you specify the data file and test file. <datafile testfile>   ")
    }
    import spark.implicits._
    val df: Dataset[Flight] = spark.read.format("json").option("inferSchema", "false").schema(schema).load(file).as[Flight]

    df.show

    val df1 = df.withColumn("orig_dest", concat($"src", lit("_"), $"dst"))
    df1.show()
    df1.createOrReplaceTempView("flights")

    val delaybucketizer = new Bucketizer().setInputCol("depdelay").setOutputCol("delayed").setSplits(Array(0.0, 41.0, Double.PositiveInfinity))
    val df2 = delaybucketizer.transform(df1)
    df2.cache


    df2.createOrReplaceTempView("flights")

  

    val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3), 5043)

    // keep all delayed , keep 13% not delayed
    val fractions = Map(0.0 -> .13, 1.0 -> 1.0) // 
    val df3 = df2.stat.sampleBy("delayed", fractions, 36L)
    // original distribution
    df2.groupBy("delayed").count.show
    // now
    df3.groupBy("delayed").count.show

    // categorical Column names
    val categoricalColumns = Array("carrier", "src", "dst", "dofW", "orig_dest")

    // a StringIndexer will encode a string categorial column into a column of numbers
    val stringIndexers = categoricalColumns.map { colName =>
      new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(colName + "Indexed")
        .fit(trainingData)
    }

    //a Bucketizer is used to add a label column of delayed 0/1.
    val labeler = new Bucketizer().setInputCol("depdelay").setOutputCol("label").setSplits(Array(0.0, 40.0, Double.PositiveInfinity))

    // list of all the feature columns
    val featureCols = Array("carrierIndexed", "dstIndexed", "srcIndexed", "dofWIndexed", "orig_destIndexed", "crsdephour", "crsdeptime", "crsarrtime", "crselapsedtime", "dist")

    //The VectorAssembler combines a given list of columns into a single feature vector column. 
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")

    // The final element in our ml pipeline is an estimator (a random forest classifier), 
    // which will training on the vector of label and features.
    val rf = new RandomForestClassifier().setLabelCol("label").setFeaturesCol("features").setNumTrees(10).setMaxBins(1000).setMaxDepth(8)

    // Below we chain the stringindexers, vector assembler and randomforest in a Pipeline.
    val steps = stringIndexers ++ Array(labeler, assembler, rf)
    val pipeline = new Pipeline().setStages(steps)

    val model = pipeline.fit(trainingData)

    // Print out the feature importances
    val rfm = model.stages.last.asInstanceOf[RandomForestClassificationModel]

    val featureImportances = rfm.featureImportances
    assembler.getInputCols.zip(featureImportances.toArray).sortBy(-_._2).foreach { case (feat, imp) => println(s"feature: $feat, importance: $imp") }

    val predictions = model.transform(testData)

    val evaluator = new BinaryClassificationEvaluator()
    val areaUnderROC = evaluator.evaluate(predictions)
    println("areaUnderROC " + areaUnderROC)

    val lp = predictions.select("prediction", "label")
    val counttotal = predictions.count().toDouble
    val correct = lp.filter("label == prediction").count().toDouble
    val wrong = lp.filter("label != prediction").count().toDouble
    val ratioWrong = wrong / counttotal
    val ratioCorrect = correct / counttotal
    val truen = (lp.filter($"label" === 0.0).filter($"label" === $"prediction").count()) / counttotal
    val truep = (lp.filter($"label" === 1.0).filter($"label" === $"prediction").count()) / counttotal
    val falsen = (lp.filter($"label" === 0.0).filter(not($"label" === $"prediction")).count()) / counttotal
    val falsep = (lp.filter($"label" === 1.0).filter(not($"label" === $"prediction")).count()) / counttotal

    println("ratio correct", ratioCorrect)

    println("true positive", truep)

    println("false positive", falsep)

    println("true negative", truen)

    println("false negative", falsen)
 
    model.write.overwrite().save(modeldirectory)

  }
}

