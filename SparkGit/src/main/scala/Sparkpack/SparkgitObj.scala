package Sparkpack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._

object SparkgitObj {
  def main(args: Array[String]): Unit = {

    println("Hello World")

    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.textFile("file:///home/cloudera/data/txns")

    val gymdata = data.filter(x => x.contains("Gymnastics"))

    "hadoop fs -rmr /user/cloudera/gymdata_dir" !

    gymdata.saveAsTextFile("hdfs:/user/cloudera/gymdata_dir")

  }
}