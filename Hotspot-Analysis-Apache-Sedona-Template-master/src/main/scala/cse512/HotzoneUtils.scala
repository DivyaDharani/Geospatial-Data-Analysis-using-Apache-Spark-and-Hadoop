package cse512

import org.apache.log4j.{Level, Logger}

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    
    // YOU NEED TO CHANGE THIS PART
    var rect : Array[Double] = queryRectangle.split(",").map(x => x.toDouble)
    var point : Array[Double] = pointString.split(",").map(x => x.toDouble)

    var x_1 = math.min(rect(0), rect(2))
    var x_2 = math.max(rect(0), rect(2))

    var y_1 = math.min(rect(1), rect(3))
    var y_2 = math.max(rect(1), rect(3))

    //checking whether the point is inside the rectangle or not
    if(point(0) >= x_1 && point(0) <= x_2 && point(1) >= y_1 && point(1) <= y_2)
      return true
    else
      return false
  }

  // YOU NEED TO CHANGE THIS PART IF YOU WANT TO ADD ADDITIONAL METHODS

}
