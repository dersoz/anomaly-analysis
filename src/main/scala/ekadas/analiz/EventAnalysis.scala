package ekadas.analiz

import org.apache.spark.sql.SparkSession

object EventAnalysis {
  val inputFileName: String = "input/events.csv"

  def main(args: Array[String]): Unit = {
    val ss = buildSparkSession
    val df = ss.read.csv(inputFileName)
    // FIXME: Needs encoder ...
    val dff = df.as[EventLog]
  }

  private def buildSparkSession = {
    val ss = SparkSession.builder()
      .appName("AnomalyAnalyzer")
      .master("local")
      .getOrCreate()
    ss
  }


}
