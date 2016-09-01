
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object SentenceCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\winutils")

    val sc = new SparkContext(conf)
    val textFile: RDD[String] = sc.textFile("data/input/sentencesFile.txt")

    val LineCount=textFile.map(s=>(s,1)).reduceByKey((a,b)=>(a+b))
    val text= LineCount.coalesce(1).saveAsTextFile("LineCount.txt")

    val sortedLines= textFile.map(s=>(s,1)).reduceByKey((a,b)=>a+b).sortByKey()
    val sortedtext= sortedLines.coalesce(1).saveAsTextFile("SortedLines.txt")

    val totalNumSentences=textFile.map(line => line).count()


  }
}

