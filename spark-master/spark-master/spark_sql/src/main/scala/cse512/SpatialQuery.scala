package cse512

import org.apache.spark.sql.SparkSession
import scala.math


object SpatialQuery extends App {
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register(
      "ST_Contains",
      (queryRectangle: String, pointString: String) =>
        (stContains(pointString, queryRectangle))
    )

    val resultDf = spark.sql(
      "select * from point where ST_Contains('" + arg2 + "',point._c0)"
    )
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(
      spark: SparkSession,
      arg1: String,
      arg2: String
  ): Long = {

    val pointDf = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register(
      "ST_Contains",
      (queryRectangle: String, pointString: String) =>
        (stContains(pointString, queryRectangle))
    )

    val resultDf = spark.sql(
      "select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)"
    )
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(
      spark: SparkSession,
      arg1: String,
      arg2: String,
      arg3: String
  ): Long = {

    val pointDf = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register(
      "ST_Within",
      (pointString1: String, pointString2: String, distance: Double) =>
        (stWithin(pointString1, pointString2, distance))
    )

    val resultDf = spark.sql(
      "select * from point where ST_Within(point._c0,'" + arg2 + "'," + arg3 + ")"
    )
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(
      spark: SparkSession,
      arg1: String,
      arg2: String,
      arg3: String
  ): Long = {

    val pointDf = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register(
      "ST_Within",
      (pointString1: String, pointString2: String, distance: Double) =>
        (stWithin(pointString1, pointString2, distance))
    )
    val resultDf = spark.sql(
      "select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, " + arg3 + ")"
    )
    resultDf.show()

    return resultDf.count()
  }

  def stContains(pointString: String, queryRectangle: String): Boolean = {
    // parse the point string
    val pt   = pointString.split(",")
    val pt_x = pt(0).trim().toDouble
    val pt_y = pt(1).trim().toDouble

    // parse the query rectangle
    val rect    = queryRectangle.split(",")
    val rect_x1 = rect(0).trim().toDouble
    val rect_y1 = rect(1).trim().toDouble
    val rect_x2 = rect(2).trim().toDouble
    val rect_y2 = rect(3).trim().toDouble

    var min_x: Double = 0
    var max_x: Double = 0
    var min_y: Double = 0
    var max_y: Double = 0

    // handle orientation of the rectangle on the x axis
    if (rect_x1 < rect_x2) {
      min_x = rect_x1
      max_x = rect_x2
    } else {
      min_x = rect_x2
      max_x = rect_x1
    }

    // handle orientation of the rectangle on the y axis
    if (rect_y1 < rect_y2) {
      min_y = rect_y1
      max_y = rect_y2
    } else {
      min_y = rect_y2
      max_y = rect_y1
    }

    // determine if the point is within the query rectangle
    if (pt_x >= min_x && pt_x <= max_x && pt_y >= min_y && pt_y <= max_y) {
      return true
    } else {
      return false
    }
  }

  // TODO
  def stWithin(
      pointString1: String,
      pointString2: String,
      distance: Double
  ): Boolean = {

    // parse pointString1 & pointString2
    val pt1   = pointString1.split(",")
    val pt1_x = pt1(0).trim().toDouble
    val pt1_y = pt1(1).trim().toDouble

    val pt2   = pointString2.split(",")
    val pt2_x = pt2(0).trim().toDouble
    val pt2_y = pt2(1).trim().toDouble

    val d = distance
    return math.sqrt((math.pow((pt1_x - pt2_x), 2)) + (math.pow((pt1_y - pt2_y), 2)))  <= d
  }
}

