package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    var rect:Array[String] = queryRectangle.split(",")
    var parsedRect:Array[Double] = rect.map(_.toDouble)
    var pt:Array[String] = pointString.split(",")
    var parsedPt:Array[Double] = pt.map(_.toDouble)

    // find rectangle min/max x
    var min_x = if (parsedRect(0) < parsedRect(2)) parsedRect(0) else parsedRect(2)
    var max_x = if (parsedRect(0) > parsedRect(2)) parsedRect(0) else parsedRect(2)

    // find rectangle min/max y
    var min_y = if (parsedRect(1) < parsedRect(3)) parsedRect(1) else parsedRect(3)
    var max_y = if (parsedRect(1) > parsedRect(3)) parsedRect(1) else parsedRect(3)

    return (parsedPt(0) >= min_x && parsedPt(0) <= max_x && parsedPt(1) >= min_y && parsedPt(1) <= max_y)
  }
}
