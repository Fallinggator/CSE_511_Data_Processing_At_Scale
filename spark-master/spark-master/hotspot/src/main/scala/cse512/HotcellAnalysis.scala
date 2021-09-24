package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  private val log = Logger.getLogger("CSE511-Hotcell-Analysis")

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    log.info("Simplified PickupInfo")
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    val cell_boundaries: Seq[Int] = Seq(minX.toInt, maxX.toInt, minY.toInt, maxY.toInt, minZ.toInt, maxZ.toInt)
    pickupInfo.createOrReplaceTempView("pickupInfo")

    // count the number of cells within boundaries
    val counts = spark.sql("select x, y, z, count(*) as points from pickupInfo group by x,y,z")
      .filter(r=>HotcellUtils.in_bounds(r, cell_boundaries))
    counts.createOrReplaceTempView("counts")

    val count_array = counts.rdd.collect()
    val mean = HotcellUtils.calculate_mean(count_array, numCells)
    val std = HotcellUtils.calculate_standard_deviation(count_array, numCells, mean)
    val g_score: RDD[Row] = counts.rdd.map(r => HotcellUtils.calculate_g_score(r, count_array, numCells, mean, std, cell_boundaries))

    val schema = new StructType()
      .add(StructField("x", IntegerType))
      .add(StructField("y", IntegerType))
      .add(StructField("z", IntegerType))
      .add(StructField("g_score", DoubleType))

    val finalDf = spark.createDataFrame(g_score, schema).orderBy(desc("g_score")).coalesce(1)
    finalDf.createOrReplaceTempView("finalDf")

    log.info("finalDf with gscore")
    finalDf.show()

    // reduce to specified new coordinates defined above (e.g. x,y,z)
    return finalDf.select(newCoordinateName.head, newCoordinateName.tail: _*).coalesce(1)
  }
}
