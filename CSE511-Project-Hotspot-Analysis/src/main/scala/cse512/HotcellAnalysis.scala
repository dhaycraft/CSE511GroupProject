package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
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
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  pickupInfo.createOrReplaceTempView("pickup_coords")
  val countCells = spark.sql(s"""
    SELECT x,y,z, COUNT(*) AS countPoints
    FROM pickup_coords
    WHERE x BETWEEN $minX AND $maxX AND
    y BETWEEN $minY and $maxY AND
    z BETWEEN $minZ and $maxZ
    GROUP BY x,y,z
    """)

  countCells.createOrReplaceTempView("countCells_tbl")
  countCells.show()
  // Calculate mean and standard deviation
  val avgCount: Double = countCells.agg(sum("countPoints")/numCells).first.getDouble(0)
  
  val sdCount: Double = math.sqrt(countCells.agg(sum(pow("countPoints",2)) / numCells-math.pow(avgCount,2)).first.getDouble(0))
  

  // get neighbors
  spark.udf.register("areNeighbors", (x1: Double, y1: Double, z1: Double, x2: Double, y2: Double, z2: Double) =>
      HotcellUtils.areNeighbors(x1=x1, y1=y1, z1=z1, x2=x2,y2=y2, z2=z2))

  val getNeighbors = spark.sql(s"""
    SELECT cc1.x, cc1.y, cc1.z, SUM(cc2.countPoints) AS spatial_weight
    FROM countCells_tbl AS cc1,
         countCells_tbl as cc2
    WHERE areNeighbors(cc1.x,  cc1.y,  cc1.z,  cc2.x, cc2.y, cc2.z)
    GROUP BY cc1.x, cc1.y, cc1.z
    """)

  getNeighbors.show()
  getNeighbors.createOrReplaceTempView("spatial_weights_tbl")
  
  spark.udf.register("numberOfCells", (x: Double, y: Double, z: Double, minX: Double, minY: Double, minZ: Double, maxX: Double, maxY: Double, maxZ: Double) =>
    HotcellUtils.numberOfCells(x=x, y=y, z=z, minX=minX, minY=minY, minZ=minZ, maxX=maxX, maxY=maxY, maxZ=maxZ))
  
  val adjacentCells = spark.sql(s"""
    SELECT x,y,z,spatial_weight, numberOfCells(x,y,z,$minX, $minY,$minZ, $maxX, $maxY, $maxZ) AS number_of_cells
    FROM spatial_weights_tbl
    """)
  adjacentCells.show()
  adjacentCells.createOrReplaceTempView("adjacent_cells_tbL")
  
  return countCells // YOU NEED TO CHANGE THIS PART
}
}
