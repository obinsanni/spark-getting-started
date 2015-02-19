package com.example.spark.demo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Created by wma on 2/18/15.
 */
object SparkDemo {
  val conf = new SparkConf().setMaster("local").setAppName("SparkDemo")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val sampleRdd = sc.parallelize(List(1,2,3,4,5,6))
    println("Length of sample RDD is " + sampleRdd.count)
  }

  def getIntRdd (length: Int): RDD[Int] = {
    sc.parallelize(1 to length)
  }
}
