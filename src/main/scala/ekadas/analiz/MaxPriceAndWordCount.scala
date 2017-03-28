package ekadas.analiz

import org.apache.spark.{SparkConf, SparkContext}

object MaxPriceAndWordCount {

  val outputFileName: String = "outs/priceOut"
  val countOutFileName: String = "outs/countOut"
  val inputFileName: String = "input/table.csv"

  def main(args: Array[String]) {
    val sc: SparkContext = buildSparkContext
    countWordsInFile(sc, inputFileName)
    calculateMaxPriceByYear(sc, inputFileName, outputFileName)
  }

  private def buildSparkContext = {
    val conf = new SparkConf()
      .setAppName("MaxPriceCalculator")
      .setMaster("local")
    val sc = new SparkContext(conf)
    sc.cancelAllJobs()
    sc
  }

  private def configureSparkApp(conf: SparkConf) = {
    conf
      .set("spark.cores.max", "1")
      .set("spark.executor.cores", "1")
      .set("spark.driver.cores", "1")
      .set("spark.cores.max", "1")
      .set("spark.driver.memory", "512m")
      .set("spark.executor.memory", "512m")
  }

  private def calculateMaxPriceByYear(sc: SparkContext, fileName: String, outputFileName: String) = {
    sc.textFile(fileName)
      .map(_.split(","))
      .map(rec => (rec(0).split("-")(0).toInt, rec(1).toFloat))
      .reduceByKey((a, b) => Math.max(a, b))
      .saveAsTextFile(outputFileName)
  }

  private def countWordsInFile(sc: SparkContext, txt: String) = {
    try {
      val counts = sc
        .textFile(txt)
        .flatMap(line => line.split(Array(' ', ',')))
        .map(word => (word, 1))
        .reduceByKey(_ + _)
        .saveAsTextFile(countOutFileName)
    } catch {
      case e: Exception => Seq[String]()
        println(e)
        sc.stop
        sys.exit(1)
    }
  }

}
