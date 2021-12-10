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
  //pickupInfo.show()

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
  //pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  pickupInfo.createOrReplaceTempView("pickup_view")
  //pickupInfo.show()
  //filter the points of interest based on the x, y and z coordinates given
  pickupInfo = spark.sql(s"select x,y,z from pickup_view where x>= $minX and x<= $maxX and y>= $minY and y<= $maxY and z>= $minZ and z<= $maxZ order by z,y,x")
  pickupInfo.createOrReplaceTempView("filtered_values")
  //pickupInfo.show()
  //aggregate the x,y,z coordinates by count. The resulting dataframe represents how many pickups occurred at given coordinates
  pickupInfo = spark.sql("select x, y, z, count(*) as hot_cells from filtered_values group by x, y, z order by z,y,x")
  pickupInfo.createOrReplaceTempView("filtered_hot_cells")

  //==========this section is to calculate the standard diviation of the pickup count values that will be used in G-score calulation
  //summing all the counts. sum_filtered_cells represents the number of all pickups.
  val sum_filtered_cells = spark.sql("select sum(hot_cells) as summed_hot_cells from filtered_hot_cells")
  //sum_filtered_cells.show()
  sum_filtered_cells.createOrReplaceTempView("sum_filtered_cells")
  val mean = (sum_filtered_cells.first().getLong(0).toDouble / numCells).toDouble
  spark.udf.register("squared", (input: Int) => (((input*input).toDouble)))
  val sum_square = spark.sql("select sum(squared(hot_cells)) as sum_square from filtered_hot_cells")
  sum_square.createOrReplaceTempView("sum_square")
  //println(mean)
  //sum_square.show()
  val SD = scala.math.sqrt(((sum_square.first().getDouble(0).toDouble / numCells.toDouble) - (mean.toDouble * mean.toDouble))).toDouble
  //print(SD)
  //==========end of the section
  //define the function for calculating the number of neighboring cells for given coordinates
  spark.udf.register("neighbor_cells", (x_val: Int, y_val: Int, z_val: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int) => ((HotcellUtils.neighbor_cells(x_val, y_val, z_val, minX, maxX, minY, maxY, minZ, maxZ))))
  //aggregating the neighboring cells' pickups for each coordinate
  val neighbor_cells = spark.sql(s"" +
    s"select neighbor_cells(table1.x, table1.y, table1.z, $minX,$maxX,$minY,$maxY,$minZ,$maxZ) as neighbor_cell_count, " +
            s"table1.x as x, table1.y as y, table1.z as z, " +
            s"sum(table2.hot_cells) as sumHotCells " +
    s"from filtered_hot_cells as table1, filtered_hot_cells as table2 " +
    s"where (table2.x = table1.x+1 or table2.x = table1.x or table2.x = table1.x-1) and " +
          s"(table2.y = table1.y+1 or table2.y = table1.y or table2.y = table1.y-1) and " +
          s"(table2.z = table1.z+1 or table2.z = table1.z or table2.z = table1.z-1) " +
    s"group by table1.z, table1.y, table1.x order by table1.z, table1.y, table1.x")
  //neighbor_cells.show()
  neighbor_cells.createOrReplaceTempView("neighbor_cells")
  //define the g-score function and calculate g-score
  spark.udf.register("gScore", (neighbor_cell_count: Int, sumHotCells: Int, numCells: Int, x: Int, y: Int, z: Int, mean: Double, SD: Double) => ((HotcellUtils.gScore(neighbor_cell_count, sumHotCells, numCells, x, y, z, mean, SD))))
  pickupInfo = spark.sql(s"select gScore(neighbor_cell_count, sumHotCells, $numCells, x, y, z, $mean, $SD) as GOS, x, y, z from neighbor_cells order by GOS desc")
  //pickupInfo.show()
  pickupInfo.orderBy(desc("GOS"),desc("x"),desc("y"),desc("z"))
  pickupInfo.createOrReplaceTempView("GOS")
  pickupInfo = spark.sql("select x, y, z from GOS")
  //pickupInfo.show()
  return pickupInfo
}
}
