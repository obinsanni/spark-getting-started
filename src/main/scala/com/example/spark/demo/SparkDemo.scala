package com.example.spark.demo

import scala.io.Source

import java.text.SimpleDateFormat
import java.util.Date

import com.datastax.spark.connector._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Created by wma on 2/18/15.
 */
object SparkDemo {
//  val conf = new SparkConf().setMaster("local").setAppName("SparkDemo")
  val conf = new SparkConf(true).setMaster("local").setAppName("SparkDemo").set("spark.cassandra.connection.host", "127.0.0.1")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val sampleRdd = sc.parallelize(List(1,2,3,4,5,6))
    println("Length of sample RDD is " + sampleRdd.count)

    val waterData = getWaterQualityData("usgs_09085100.txt")
    val sum = waterData.aggregate((0, 0))((acc, value) => (acc._1 + value._3, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val avgDischargeRate = sum._1/sum._2
    println("Average discharge rate over the 4-year period: " + avgDischargeRate + " cf/sec")

    // create a couple of output files:
    waterData.saveAsObjectFile("waterDataObj")
    waterData.saveAsTextFile("waterDataTxt")

    val objRdd = sc.objectFile("waterDataObj")
    println("\ncount of reconstituted object RDD: " + objRdd.count)
    val txtRdd = sc.textFile("waterDataTxt")
    println("count of reconstituted text RDD: " + txtRdd.count)

    println("repeat, using HDFS:")
    waterData.saveAsObjectFile("hdfs://localhost:9000/kitchenSink/waterDataObj")
    waterData.saveAsTextFile("hdfs://localhost:9000/kitchenSink/waterDataTxt")
    val hdfsObjRdd = sc.objectFile("hdfs://localhost:9000/kitchenSink/waterDataObj")
    println("  count of object RDD reconstituted from HDFS: " + hdfsObjRdd.count)
    val hdfsTxtRdd = sc.textFile("hdfs://localhost:9000/kitchenSink/waterDataTxt")
    println("  count of text RDD reconstituted from HDFS: " + hdfsTxtRdd.count)

    println("one more time, this time using Cassandra:")
    waterData.saveToCassandra("waterquality", "nwis", SomeColumns("gaugelabel", "measurementdate", "dischargerate", "maxtempcelsius", "mintempcelsius"))
    val cassandraRdd = sc.cassandraTable("waterquality", "nwis")
    println("  count of RDD retrieved from Cassandra: " + cassandraRdd.count)

    sc.stop()
  }

  def getIntRdd (length: Int): RDD[Int] = {
    sc.parallelize(1 to length)
  }

  // assume file somewhere on the classpath:
  def getWaterQualityData(filename: String): RDD[(String, Date, Int, Float, Float)] = {
    val lines = Source.fromInputStream(this.getClass().getClassLoader().getResourceAsStream(filename)).getLines()
    // we could skip some number of lines, or we could just notice that the data
    // lines start with "USGS":
    val dataLines = lines.filter(line => line.startsWith("USGS"))
    // not all data is available on all lines, so we just skip the lines
    // that are not complete, with a special filter function that just looks
    // at the size of the split line:
    val fullDataLines = dataLines.filter(line => {
      val fields = line.split("\\\t", 9)
      fields.length == 9
    })
    sc.parallelize(fullDataLines.map(nextLine => extractTuple(nextLine)).toList)
  }

  def extractTuple(line: String): (String, Date, Int, Float, Float) = {
    // string is tab-delimited:
    val fields = line.split("\\\t", 9)
    (fields(1), new SimpleDateFormat("yyyy-MM-dd").parse(fields(2)), fields(3).toInt,
      fields(5).toFloat, fields(7).toFloat)
  }
}
