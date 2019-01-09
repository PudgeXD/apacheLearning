import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Kmeans {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Kmeans").setMaster("local")
        val sc = new SparkContext(conf)
        
        //k-means
        val input = sc.textFile("file:///home/hadoop/Documents/distribution/input")
        val k = input.first().split(",")(0).toInt
        val total = input.first().split(",")(1).toInt

        val doubleData = input.filter(line=>line!=input.first())
              .map(point=>point.split(","))
              .map(point=>(point(0).toDouble,point(1).toDouble))

        //随机取点集中的k个点作为起始中心点
        val centersArray = util.Random.shuffle(doubleData.collect.toList).take(k).toArray
        val centersBuffer = scala.collection.mutable.ArrayBuffer[(Double,Double)]()
        centersBuffer ++= centersArray

        //计算某点属于哪个中心点
        def divideMap(point:(Double,Double)) : ((Double,Double), (Double,Double)) = {
        var minimum = 2.0/0
        var key = (0.0,0.0)
        for(pos <- 0 to k-1) {
            var tmp = distance(point,centersBuffer(pos))
            if(tmp < minimum) {
                minimum = tmp
                key = centersBuffer(pos)
            }
        }
        return (key,point)
    }
        /*def run(k:Int) : Iterable[(Double,Double)] = {
        
        }*/
}
    //def run
    //计算点之间距离函数
    def distance(a:(Double,Double),b:(Double,Double)) : Double = {
        return math.pow(a._1-b._1,2)+math.pow(a._2-b._2,2)
    }
    
    def itrGetMean(input : Iterable[(Double,Double)]) : (Double,Double) = {
        val itr = input.iterator
          val x = scala.collection.mutable.ArrayBuffer.empty[Double]
          val y = scala.collection.mutable.ArrayBuffer.empty[Double]
          while(itr.hasNext) {
                val point = itr.next()
                x += point._1
                y += point._2
          }
          return (x.sum/x.size,y.sum/y.size)
    }
}