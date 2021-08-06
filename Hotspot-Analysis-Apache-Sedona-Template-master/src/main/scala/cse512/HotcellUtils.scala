package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
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

  // YOU NEED TO CHANGE THIS PART
  def isCellWithinBounds(x: Double, y: Double, z: Int, minX: Double, maxX: Double, minY: Double, maxY: Double, minZ: Int, maxZ: Int): Boolean = {
      return (x >= minX && x <= maxX && y >= minY && y <= maxY && z >= minZ && z <= maxZ)
  }

  def getCountOfNeighborCells(x: Double, y: Double, z: Int, minX: Double, maxX: Double, minY: Double, maxY: Double,
                              minZ: Int, maxZ: Int): Int = {
    var boundaryCount = 0
    boundaryCount += (if(x == minX || x == maxX) 1 else 0)
    boundaryCount += (if(y == minY || y == maxY) 1 else 0)
    boundaryCount += (if(z == minZ || z == maxZ) 1 else 0)

    //cell (or cube) locations: inside(no sides exposed), face(exactly one side exposed), edge(exactly two sides exposed),
    //corner(3 sides exposed)
    val cellLocationMap: Map[Int, String] = Map(0 -> "inside", 1 -> "face", 2 -> "edge", 3 -> "corner")
    val neighborCountMap: Map[String, Int] = Map("inside" -> 26, "face" -> 17, "edge" -> 11, "corner" -> 7)

    var locationOfGivenCell = cellLocationMap.get(boundaryCount).get
    var neighborCellCount = neighborCountMap.get(locationOfGivenCell).get

    return neighborCellCount
  }

  /*def calculateGStatisticScore(sumOfNeighborPickups: Int, numOfNeighborCells: Int, mean: Double, s: Double, numCells: Int): Double = {
    var numerator = sumOfNeighborPickups - (mean * numOfNeighborCells)
    var denominator = s * math.sqrt(((numCells * numOfNeighborCells) - (numOfNeighborCells * numOfNeighborCells))/(numCells - 1))
    var gScore = numerator / denominator
    return gScore
  }*/

  def calculateGStatisticScore(sumOfNeighborPickups: Int, numOfNeighborCells: Int, mean: Double, s: Double, numCells: Int): Double ={
    val numerator = (sumOfNeighborPickups.toDouble - (mean * numOfNeighborCells.toDouble))
    val denominator = s * math.sqrt((((numCells.toDouble * numOfNeighborCells.toDouble) - (numOfNeighborCells.toDouble * numOfNeighborCells.toDouble)) / (numCells.toDouble-1.0).toDouble).toDouble).toDouble
    return (numerator/denominator).toDouble
  }
}