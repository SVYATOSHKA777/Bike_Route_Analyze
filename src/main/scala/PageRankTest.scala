package main.scala
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import org.apache.spark.sql.functions._

object PageRankTest{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SparkGraphX")
      .set("spark.executor.memory", "2g")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)



    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-sql-basic")
      .master("local[*]")
      .getOrCreate()

    //Read stations
    val stations = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .csv("data/station_data.csv")

    //Read trips
    val trips = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .csv("data/trip_data.csv")

    //stations.show()

    val stationVertices = stations.withColumnRenamed("station_id", "id").distinct()
    val tripEdges = trips
      .withColumnRenamed("Start Terminal", "src")
      .withColumnRenamed("End Terminal", "dst")

    val stationGraph = GraphFrame(stationVertices, tripEdges)
    stationGraph.cache()

    stationGraph.triplets.show()

    val ranks = stationGraph.pageRank.resetProbability(0.15).maxIter(10).run()

    ranks.vertices.orderBy(desc("pagerank")).show()
    //Get the the most common destinations in the dataset from location to location
    val topTrips = stationGraph
      .edges
      .groupBy("src", "dst")
      .count()
      .orderBy(desc("count"))
      .limit(10)
    val inDeg = stationGraph.inDegrees
    inDeg.orderBy(desc("inDegree")).limit(5).show()

    val outDeg = stationGraph.outDegrees
    outDeg.orderBy(desc("outDegree")).limit(5).show()
  }
}