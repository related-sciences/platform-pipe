import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val logFile = "/Users/eczech/tmp/notes.txt" // Should be some file on your system
    val conf = new SparkConf().setAppName("App").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ct = sc.textFile(logFile, 2).count()
    println("Count: " + ct)
    sc.stop()
  }
}