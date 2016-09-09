import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LabAssign {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils");

    val sparkConf = new SparkConf().setAppName("SparkTransformation").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val textfile = sc.textFile("input.csv")

    val rows = textfile.map(line => line.split(","))

    rows.map(row => row(2)).distinct.count

    val names = rows.map(name => (name(1),1))
    val number=names.map(row => row).distinct.count
    println("the distinct count value is"+number)
    val reduce = names.reduceByKey((a,b) => a + b).sortBy(_._2)
    reduce.saveAsTextFile("ouput")

    val filteredRows = textfile.filter(line => !line.contains("Count")).map(line => line.split(","))

    val result=filteredRows.map ( n => (n(1), n(4).toInt)).reduceByKey((a,b) => a + b).sortBy(_._2).foreach (println _)

  }
  }
