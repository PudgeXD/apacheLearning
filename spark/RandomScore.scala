import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object RandomScore {
  def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("RandomScore").setMaster("local")
        val sc = new SparkContext(conf)
        val input = sc.textFile("file:///home/hadoop/Documents/distribution/input")
        
        val pair = input.map(stuno=> stuno + " " + util.Random.nextInt(101))
        .saveAsTextFile("file:///home/hadoop/Documents/distribution/output")
  }
}