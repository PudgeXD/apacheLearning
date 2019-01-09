import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object GetMean {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GetMean").setMaster("local")
    val sc = new SparkContext(conf)
    val input = sc.textFile("file:///home/hadoop/Documents/distribution/input")
    val team = input.map( line=>(((line.split(" ")(0).toInt-1)/5),line.split(" ")(1).toDouble))
    .groupByKey().map(x=>(x._1,x._2.sum/x._2.size))
    .saveAsTextFile("file:///home/hadoop/Documents/distribution/output")
  }
}