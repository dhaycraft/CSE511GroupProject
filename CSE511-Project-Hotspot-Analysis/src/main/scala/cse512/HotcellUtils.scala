package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  // YOU NEED TO CHANGE THIS PART
  def areNeighbors(x1: Double, y1: Double, z1:Double, x2: Double, y2: Double, z2: Double): Boolean={
    val areNeighbors: Boolean = if(math.abs(x1-x2)<=1 && math.abs(y1-y2)<=1 && math.abs(z1-z2)<=1){
      true
    } else{
      false
    }
    return areNeighbors
  }
  // Each cell has a neighboring region which can be considered as a 3x3 cube == 27 total cells
  // Howver there are special cases when a cell resides at a boundary
  // When a cell lies on the boundary of single axis then the total cell count is 18
  // When a cell lies on two axis boundaries then 12 cells compose the neighborhood
  // When cell lies on three axis boundaries then there are only 8 cells that compose the region
  def numberOfCells(x: Double, y: Double, z: Double, minX: Double, minY: Double, minZ: Double, maxX: Double, maxY: Double, maxZ: Double): Int = {
    var boundary_count: Int = 0
    // count number of boundaries point cell is on
    if(x==minX || x == maxX){
      boundary_count = boundary_count+1
    }
    if(y==minY || y==maxY){
      boundary_count = boundary_count+1
    }
    if(z==minZ || z==maxZ){
      boundary_count = boundary_count+1
    }
    val numberOfCells: Int = boundary_count match {
      case 1 => 18
      case 2 => 12
      case 3 => 8
      case _ => 27
    }
    return numberOfCells
  }
  
  def getisOrdStatistic(adjacentCells: Double, spatial_weight: Double, numOfCells: Double, meanCount: Double, sdCount: Double): Double = {
    numerator = spatial_weight - meanCount*adjacentCells
    denominator = sdCount * math.sqrt(((numOfCells*adjacentCells)-pow(adjacentCells,2))/(numOfCells-1))
    
}
