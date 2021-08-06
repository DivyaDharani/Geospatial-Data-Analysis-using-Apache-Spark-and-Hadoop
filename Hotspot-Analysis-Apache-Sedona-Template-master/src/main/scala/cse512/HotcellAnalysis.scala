package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("csv").option("delimiter",";").option("header","false").load(pointPath);
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

    pickupInfo.createOrReplaceTempView("pickupInfoView")

    //selecting all points within the boundary (minX, maxX, minY, maxY, minZ, maxZ)
    spark.udf.register("isCellWithinBounds", (x: Double, y: Double, z: Int) =>
      (x >= minX && x <= maxX && y >= minY && y <= maxY && z >= minZ && z <= maxZ))

    var cellsWithinBoundsDf = spark.sql("select x, y, z from pickupInfoView where isCellWithinBounds(x,y,z) order by z,y,x").persist()
    cellsWithinBoundsDf.createOrReplaceTempView("cellsWithinBoundsDfView")

    //counting pickup points (calculating attribute value Xi for each cell); Xi = pickupCount of ith cell
    var pickupCountDf = spark.sql("select x,y,z,count(*) as pickupCount from cellsWithinBoundsDfView group by z,y,x order by z,y,x").persist()
    pickupCountDf.createOrReplaceTempView("pickupCountDfView")

    spark.udf.register("square", (x: Int) => (x * x).toDouble)

    //calculating Sigma Xj and Sigma Xj^2
    var sumOfPickupsDf = spark.sql("select count(*) as numOfCellsWithValues, sum(pickupCount) as sumOfAllPickups, " +
      "sum(square(pickupCount)) as squaredSumOfAllPickups from pickupCountDfView").persist()
    sumOfPickupsDf.createOrReplaceTempView("sumOfPickupsDf")
    sumOfPickupsDf.show()

    //retrieving values from the above result
    var sumOfAllPickups = sumOfPickupsDf.first().getLong(1)
    var squaredSumOfAllPickups = sumOfPickupsDf.first().getDouble(2)

    //calculating Mean(X) = (Sigma(Xj)/n) and S = sqrt(Sigma(Xj^2)/n - (Mean(X))^2)
    var mean = sumOfAllPickups / numCells
    var s = math.sqrt((squaredSumOfAllPickups / numCells) - (mean * mean))

    //For each cell x,y,z, consider the cells with x-1,x,x+1 or y-1,y,y+1, or z-1,z,z+1 as neighbors

    spark.udf.register("getCountOfNeighborCells", (x: Double, y: Double, z: Int) =>
      (HotcellUtils.getCountOfNeighborCells(x,y,z,minX,maxX,minY,maxY,minZ,maxZ)))

    var neighborsDf = spark.sql("select t1.x, t1.y, t1.z, getCountOfNeighborCells(t1.x, t1.y, t1.z) as numOfNeighborCells, " +
      "count(*) as numOfNeighborCellsWithPickups, sum(t2.pickupCount) as sumOfNeighborPickups " +
      "from pickupCountDfView t1, pickupCountDfView t2 where " +
      //"not(t2.x = t1.x and t2.y = t1.y and t2.z = t1.z) and " +
      "abs(t1.x - t2.x) <= 1 and abs(t1.y - t2.y) <= 1 and abs(t1.z - t2.z) <= 1 " +
      "group by t1.x,t1.y,t1.z").persist()
    neighborsDf.createOrReplaceTempView("neighborsInfoView")

    spark.udf.register("calculateGStatisticScore", (sumOfNeighborPickups: Int, numOfNeighborCells: Int, mean: Double, s: Double,
      numCells: Int) => (HotcellUtils.calculateGStatisticScore(sumOfNeighborPickups, numOfNeighborCells, mean, s, numCells)))

    var gScoreDf = spark.sql("select *, calculateGStatisticScore(sumOfNeighborPickups, numOfNeighborCells, " +
      mean + ", " + s + "," + numCells + ") as gScore from neighborsInfoView order by gScore desc").persist()
    gScoreDf.createOrReplaceTempView("gScoreView")
    //gScoreDf.show()

    var orderedGScoreDf = spark.sql("select x, y, z from gScoreView order by gScore desc").persist()
    return orderedGScoreDf // YOU NEED TO CHANGE THIS PART
  }
}
