package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
      val parseRectangle: Array[String]=queryRectangle.split(",")
      val pointArray: Array[String]=pointString.split(",")
      val x1: Double=parseRectangle(0).toDouble
      val y1: Double=parseRectangle(1).toDouble
      val x2: Double=parseRectangle(2).toDouble
      val y2: Double=parseRectangle(3).toDouble
      val px: Double=pointArray(0).toDouble
      val py: Double=pointArray(1).toDouble
      val xmax: Double= List(x1,x2).max
      val xmin: Double= List(x1,x2).min
      val ymax: Double= List(y1,y2).max
      val ymin: Double= List(y1,y2).min
      return (px>=xmin && px<=xmax) && (py>=ymin && py<=ymax)
  }
}
