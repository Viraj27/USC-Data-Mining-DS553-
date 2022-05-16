import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.graphframes._

import java.io.{File, PrintWriter}
import scala.collection.Map
import scala.collection.mutable.{ListBuffer, Set => MutableSet}
import scala.util.Sorting.quickSort
object task1 extends Serializable {

  def generate_edge_rdd(row : (String, Set[String]), user_bid_map : Map[String, Set[String]], user_bid_collected_data : Array[(String, Set[String])], filter_threshold : Int): ListBuffer[(String, String)]= {
    var edge_list = ListBuffer[(String, String)]()
    val business_set = user_bid_map.getOrElse(row._1, Set())
    for ((user, _) <- user_bid_collected_data){
      if (user != row._1){
        val intersection = business_set.intersect(user_bid_map.getOrElse(user, Set()))
        if (intersection.size >= filter_threshold){
          edge_list += Tuple2(user, row._1)
        }
      }

    }
    edge_list
  }

  def generate_vertex_rdd(row : (String, String)): MutableSet[String] = {
    var vertex_set = MutableSet[String]()
    vertex_set.add(row._1)
    vertex_set.add(row._2)

    vertex_set
  }



  def task1_graphframes(filter_threshold : Int, input_file : String, output_file : String, sc : SparkContext, spark : SparkSession): Unit= {

    var input_data_rdd = sc.textFile(input_file).map(row => row.split(','))
    val header         = input_data_rdd.first()
    var processed_rdd  = input_data_rdd.filter(row => row != header).map(row => (row(0), Set(row(1)))).reduceByKey((row1, row2) => row1 | row2)

    val user_bid_map   = processed_rdd.collectAsMap()
    val user_bid_collected_data = processed_rdd.collect()

    var edge_data_rdd = processed_rdd.map(row => generate_edge_rdd(row, user_bid_map, user_bid_collected_data, filter_threshold)).filter(row => row.size != 0).flatMap(row => row)
    println(edge_data_rdd.count())
    println(edge_data_rdd.take(2))
    val edge_schema   = new StructType().add(StructField("src", StringType, true)).add(StructField("dst", StringType, true))
    val edge_DF       = spark.createDataFrame(edge_data_rdd.map(row => Row(row._1, row._2)),edge_schema)

    var vertex_data_rdd = edge_data_rdd.map(row => row._1).distinct()
    val vertex_schema   = new StructType().add(StructField("id", StringType, true))
    val vertex_DF       = spark.createDataFrame(vertex_data_rdd.map(row => Row(row)), vertex_schema)
    println(vertex_data_rdd.count())
    println(vertex_data_rdd.take(2))
    val g = GraphFrame(vertex_DF,edge_DF)
    var communities = g.labelPropagation.maxIter(5).run()
    val community_map = communities.rdd.map(row => (row.getAs[Double]("label"), row.getAs[String]("id"))).groupByKey().mapValues(_.toList.sorted).collectAsMap()
    val final_communities = community_map.values.toArray.sortBy(x => (x.length, x(0)))
    val writer = new PrintWriter(new File(output_file))
    for (community <- final_communities){
      writer.write(community.map(r => "'" + r + "'").mkString(", ")+ "\n")

    }
    writer.close()
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DM_assign4_task1").setMaster("local[*]")
    val spark = SparkSession.builder.appName("DM_assign4_task1").config(conf).getOrCreate()
    val sc    = spark.sparkContext
    sc.setLogLevel("ERROR")

    val filter_threshold    = args(0).toInt
    val input_file     = args(1).toString
    val output_file    = args(2).toString
    val startTime      = System.nanoTime()
    task1_graphframes(filter_threshold, input_file, output_file, sc, spark)
    println("Duration: " + (System.nanoTime - startTime) / 1e9d)

  }
}
