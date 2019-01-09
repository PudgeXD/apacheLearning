import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Join {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RandomScore").setMaster("local")
    val sc = new SparkContext(conf)
        
    //person表里没有16号，20号数据id缺失
    val pAttrs = sc.textFile("file:///home/hadoop/Documents/distribution/person")
                   .map(line=>line.split(" "))
                   .filter(array=>array.size==3)
                   .map(array=>(array(2),Array(array(0),array(1))))
    val aAttrs = sc.textFile("file:///home/hadoop/Documents/distribution/address")
                   .map(line=> line.split(" "))
                   .map(array=>(array(0),array(1)))

    val middle = pAttrs.join(aAttrs)
                       .map(line=>line._1+" "+line._2._1(0)+" "+line._2._1(1)+" "+line._2._2)
                       .saveAsTextFile("file:///home/hadoop/Documents/distribution/output")
  }
}