package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, RowFactory}

object HotcellUtils {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  private val log = Logger.getLogger("CSE511-Hotcell-Analysis")

  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int = {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(", "").toDouble / coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")", "").toDouble / coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser(timestampString: String): Timestamp = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear(timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth(timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def in_bounds(cell: Row, bounds: Seq[Int]): Boolean = {
    val x = cell.getAs[Int](0)
    val y = cell.getAs[Int](1)
    val z = cell.getAs[Int](2)

    if ((x >= bounds.apply(0) && x <= bounds.apply(1))
      && (y >= bounds.apply(2) && y <= bounds.apply(3))
      && (z >= bounds.apply(4) && z <= bounds.apply(5))) true else false
  }

  def neighbors(cell: Row, bounds: Seq[Int]): Int = {
    val x = cell.getAs[Int](0)
    val y = cell.getAs[Int](1)
    val z = cell.getAs[Int](2)
    val x_edge = if (x <= bounds.apply(0) || x >= bounds.apply(1)) 1 else 0
    val y_edge = if (y <= bounds.apply(2) || y >= bounds.apply(3)) 1 else 0
    val z_edge = if (z == bounds.apply(4) || z == bounds.apply(5)) 1 else 0
    val total = x_edge + y_edge + z_edge

    // corner
    if (total == 3) {
      return 8
    }

    // edge
    if (total == 2) {
      return 12
    }

    // center exterior
    if (total == 1) {
      return 18
    }

    // internal
    return 27
  }

  def calculate_weight(cell: Row, other_cell: Row): Int = {
    val check_boundary = (a: Int, b: Int) => {
      (b <= a + 1 && b >= a - 1)
    }

    val is_neighbor = (check_boundary(cell.getAs[Int](0), other_cell.getAs[Int](0))
      && check_boundary(cell.getAs[Int](1), other_cell.getAs[Int](1))
      && check_boundary(cell.getAs[Int](2), other_cell.getAs[Int](2)))

      return if (is_neighbor) 1 else 0
  }

  def calculate_mean(data: Array[Row], count: Double): Double = {
    val sum = data.map(r => r.getAs[Long]("points")).reduce((i, j) => i + j)
    return sum / count
  }

  def calculate_standard_deviation(data: Array[Row], count: Double, avg: Double): Double = {
    val sum = data.map(e => math.pow(e.getAs[Long]("points"), 2)).reduce((i, j) => i + j)
    return math.sqrt((sum / count) - math.pow(avg, 2))
  }

  def calculate_g_score(cell: Row, grid: Array[Row], numCells: Double, avg: Double, s: Double, bounds: Seq[Int]): Row = {
    val sum = grid.map(r => {
      (r.getAs[Long](3) * calculate_weight(cell, r))
    }).reduce((i, j) => i + j)
    val w = neighbors(cell, bounds)
    val numerator = sum - w * avg
    val denominator = s * math.sqrt((numCells * w - math.pow(w, 2)) / (numCells - 1))
    val g_score: java.lang.Double = numerator.toDouble / denominator.toDouble
    return RowFactory.create(cell.getAs[String]("x"), cell.getAs[String]("y"), cell.getAs[String]("z"), g_score)
  }
}
