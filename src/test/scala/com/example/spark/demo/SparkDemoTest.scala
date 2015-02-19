package com.example.spark.demo

import org.scalatest.FunSpec

/**
 * Created by wma on 2/18/15.
 */
class SparkDemoTest extends FunSpec {
  describe("An RDD created with SparkDemo.getIntRdd()") {
    describe("When the length argument is 6") {
      val intRdd = SparkDemo.getIntRdd(6)
      it("should have length 6") {
        assert(intRdd.count === 6)
      }
      it("should have the sum of its elements equal 21") {
        val sum = intRdd.aggregate((0, 0))((acc, value) => (acc._1 + value, acc._2 + 1),
          (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
        assert(sum._1 === 21)
      }
      it("should have the product of its elements equal 720") {
        val prod = intRdd.aggregate((1, 0))((acc, value) => (acc._1 * value, acc._2 + 1),
          (acc1, acc2) => (acc1._1 * acc2._1, acc1._2 + acc2._2))
        assert(prod._1 === 720)
      }
    }
  }
}
