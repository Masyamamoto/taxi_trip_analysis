package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    var Array(p1, p2) = pointString.split(",")
    var Array(x1, y1, x2, y2) = queryRectangle.split(",")
    if ((x1.toDouble <= p1.toDouble && p1.toDouble <= x2.toDouble) && (y1.toDouble <= p2.toDouble && p2.toDouble <= y2.toDouble)) {
      return true
    }
    else {
      return false
    }
  }
}
