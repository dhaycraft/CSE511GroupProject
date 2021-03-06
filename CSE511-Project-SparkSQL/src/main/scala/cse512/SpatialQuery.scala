package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def scalaWithin (pointString1:String, pointString2:String, distance:Double): Boolean= {
      val pointArray1: Array[String]=pointString1.split(",")
      val pointArray2: Array[String]=pointString2.split(",")
      val x1: Double=pointArray1(0).toDouble
      val y1: Double=pointArray1(1).toDouble
      val x2: Double=pointArray2(0).toDouble
      val y2: Double=pointArray2(1).toDouble
      val distPoints: Double=scala.math.sqrt(scala.math.pow(x1-x2,2)+scala.math.pow(y1-y2,2))
      return distPoints<=distance
    }
  
  def scalaContains (queryRectangle:String, pointString:String): Boolean={
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

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>{
      scalaContains(queryRectangle = queryRectangle,pointString = pointString)
    })

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }
  
  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>{
      scalaContains(queryRectangle = queryRectangle,pointString = pointString)
    })

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>{
      scalaWithin(pointString1 = pointString1, pointString2=pointString2, distance=distance)
  })

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>{
      scalaWithin(pointString1 = pointString1, pointString2=pointString2, distance=distance)
  })

    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
