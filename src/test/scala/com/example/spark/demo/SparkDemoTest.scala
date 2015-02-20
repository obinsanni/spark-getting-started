package com.example.spark.demo

import java.text.SimpleDateFormat

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

  describe("The extractTuple() method") {
    describe("When given an incomplete line of data") {
      it("should throw an ArrayIndexOutOfBoundsException") {
        intercept[ArrayIndexOutOfBoundsException] {
          val incompleteLine = "USGS\t09085100\t1980-05-04\t5290\tA"
          val shouldFail = SparkDemo.extractTuple(incompleteLine)
        }
      }
    }
    describe("When given a complete line of sample data") {
      it("should parse the line correctly") {
        val completeLine = "USGS\t09085100\t1980-05-03\t5020\tA\t9.0\tA\t7.5\tA"
        val measurementDate = new SimpleDateFormat("yyyy-MM-dd").parse("1980-05-03")
        val parsedTuple = SparkDemo.extractTuple(completeLine)
        assert(parsedTuple._1 === "09085100" &&
          parsedTuple._2.compareTo(measurementDate) === 0 &&
          parsedTuple._3 === 5020 &&
          parsedTuple._4.compareTo("9.0".toFloat) === 0 &&
          parsedTuple._5.compareTo("7.5".toFloat) === 0)
      }
    }
  }

  describe("The getWaterQualityData() method") {
    describe("when given the sample file") {
      val dataSet = SparkDemo.getWaterQualityData("usgs_09085100.txt")
      it("should return 1487 complete records") {
        assert(dataSet.count === 1487)
      }
      it("should only contain records for sensor 09085100") {
        val badLines = dataSet.filter(tuple => tuple._1 != "09085100")
        assert(badLines.count === 0)
      }
    }
  }
}
