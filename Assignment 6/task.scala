import scala.collection.mutable._
import scala.collection.mutable.{ListBuffer, Set => MutableSet}
import scala.io.Source
import scala.util.Random
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.io.{File, PrintWriter}
import scala.collection.mutable

object task extends Serializable {

  class Cluster(points_list: ListBuffer[ListBuffer[Double]]) extends Serializable {
    //var points_list : ListBuffer[ListBuffer[Double]] = points_list
    var pnts_list: ListBuffer[ListBuffer[Double]] = points_list
    val length: Integer = pnts_list(0).length
    var features_data: ListBuffer[ListBuffer[Double]] = pnts_list.map(x => x.slice(2, length))
    var n: Integer = pnts_list.length
    var sum_list : ListBuffer[Double] = ListBuffer[Double]()
    var sum_square_list : ListBuffer[Double] = ListBuffer[Double]()
    var centroid = ListBuffer[Double]()
    var standard_deviation = ListBuffer[Double]()

    // calculate sum_list
    sum_list = features_data.transpose.map(_.sum)

    // ************calculate sum_square_list***************
    sum_square_list = features_data.transpose.map(x => x.map(y => y * y).sum)

    def calculate_centroid(sum_list : ListBuffer[Double], n : Integer):Unit= { // calculate centroid
      for (x <- sum_list) {
        centroid.append(x / n)
      }
    }
    calculate_centroid(sum_list, n)

    def calculate_standard_deviation(sum_square_list : ListBuffer[Double], n : Integer, centroid : ListBuffer[Double]):Unit={}
    // calculate standard deviation
    for (i <- 0 until sum_square_list.length){
      val sd = math.sqrt((sum_square_list(i) / n) - math.pow(centroid(i), 2))
      standard_deviation.append(sd)
    }
    calculate_standard_deviation(sum_square_list, n, centroid)

    def add_point(point : ListBuffer[Double]):Unit={
      pnts_list.append(point)
      val features_data = point.slice(2, point.length)
      n += 1
      for (i <- 0 until features_data.length){
        sum_list(i) += features_data(i)
        sum_square_list(i) += features_data(i) * features_data(i)
      }
      calculate_centroid(sum_list, n)
      calculate_standard_deviation(sum_square_list, n, centroid)
    }

  }

  def perform_K_Means(data : ListBuffer[ListBuffer[Double]], sc : SparkContext, input_clusters : Integer):Array[Int]={

    val features_data = data.map(x => x.slice(2, data.length))
    val dataRDD = sc.parallelize(features_data.map(x=> Vectors.dense(x.toArray)))
    val kmeans = new KMeans().setK(input_clusters).setMaxIterations(300).setInitializationSteps(10)
    val model = kmeans.run(dataRDD)
    val predictions = model.predict(dataRDD).collect()

    predictions
  }

  def cluster_points_map(data : ListBuffer[ListBuffer[Double]], K_means_results : Array[Int], pred_length : Integer):mutable.Map[Integer, ListBuffer[ListBuffer[Double]]]={
    var cluster_point_map = mutable.Map[Integer, ListBuffer[ListBuffer[Double]]]().withDefaultValue(ListBuffer[ListBuffer[Double]]())

    for (idx <- 0 until pred_length){
      val cluster = K_means_results(idx)
      cluster_point_map.update(cluster, cluster_point_map(cluster) :+ data(idx))
    }
    cluster_point_map
  }

  def calculate_mahalanobis_distance(data_source_1 : ListBuffer[Double], data_source_2 : Cluster):Double={
    var maha_dist_list = ListBuffer[Double]()
    for (i <- 0 until data_source_1.length){
      maha_dist_list.append(0.0)
    }
    for (i <- 0 until data_source_1.length){
      val calc = math.pow((data_source_1(i) - data_source_2.centroid(i)) / data_source_2.standard_deviation(i), 2)
      maha_dist_list(i) += calc
    }
    math.sqrt(maha_dist_list.sum)
  }

  def calculate_mahalanobis_distance_between_clusters(data_source_1 : Cluster, data_source_2 : Cluster):Double={
    var maha_dist_list = ListBuffer[Double]()
    val cluster_1_centroid = data_source_1.centroid
    val cluster_2_centroid = data_source_2.centroid
    for (i <- 0 until cluster_1_centroid.length){
      maha_dist_list.append(0.0)
    }
    for (i <- 0 until cluster_1_centroid.length){
      val calc = math.pow((cluster_1_centroid(i) - cluster_2_centroid(i)) / data_source_1.standard_deviation(i), 2)
      maha_dist_list(i) += calc
    }
    math.sqrt(maha_dist_list.sum)
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DM_assign4_task1").setMaster("local[*]")
    val spark = SparkSession.builder.appName("DM_assign4_task1").config(conf).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val input_file = args(0).toString
    val input_clusters = args(1).toInt
    val output_file = args(2).toString
    val writer = new PrintWriter(new File(output_file))
    var input_data = ListBuffer[ListBuffer[Double]]()
    var retained_list = ListBuffer[ListBuffer[Double]]()
    var discard_list = ListBuffer[Cluster]()
    var compression_list = ListBuffer[Cluster]()

    var lines = Source.fromFile(input_file).getLines().toList


    for (line <- lines) {
      val line_list = line.stripSuffix("\n").split(",").to[ListBuffer]
      input_data.append(line_list.map(x => x.toDouble))
    }

    val dataset_len = input_data.length
    var random_chunk_size = math.ceil(0.2 * dataset_len)
    var train_data = ListBuffer[ListBuffer[Double]]()

    //step 1. Randomly generate 20% data.
    val r = scala.util.Random
    var shuffled_input_data = r.shuffle(input_data)
    train_data = shuffled_input_data.slice(0, random_chunk_size.toInt)

    // step 2. K-Means

    var K_means_results = perform_K_Means(train_data, sc, input_clusters * 10)
    var pred_length = K_means_results.length
    var cluster_point_map = cluster_points_map(train_data, K_means_results, pred_length)

    //step 3. Move all clusters with one point to RS.
    var remaining_data_points = ListBuffer[ListBuffer[Double]]()
    for ((cluster, points_set) <- cluster_point_map) {
      if (points_set.length == 1) {
        retained_list.append(points_set(0))
      }
      else {
        for (point <- points_set) {
          remaining_data_points.append(point)
        }
      }
    }
    // step 4. K-means on rest of data points.
    var rem_K_means_results = perform_K_Means(remaining_data_points, sc, input_clusters)
    var rem_pred_length = rem_K_means_results.length

    var rem_cluster_point_map = cluster_points_map(remaining_data_points, rem_K_means_results, rem_pred_length)

    // step 5. Create Discard Set (List).

    for ((cluster, points_list) <- rem_cluster_point_map) {
      discard_list.append(new Cluster(points_list))
    }

    writer.write("The intermediate results:\n")
    var discard_num_points = 0
    var compression_num_points = 0
    for (cluster <- discard_list) {
      discard_num_points += cluster.n
    }
    for (cluster <- compression_list) {
      compression_num_points += cluster.n
    }
    val s = Array(discard_num_points.toString, compression_list.length.toString, compression_num_points.toString, retained_list.length.toString).mkString(",")
    writer.write("Round 1: " + s + "\n")

    // step 6.

    val ret_saved_list = retained_list
    if (input_clusters * 10 < ret_saved_list.length) {
      val ret_K_means_results = perform_K_Means(ret_saved_list, sc, input_clusters * 10)
      val ret_pred_length = ret_K_means_results.length
      var ret_cluster_point_map = cluster_points_map(ret_saved_list, rem_K_means_results, ret_pred_length)
      retained_list.clear()
      for ((_, points_set) <- ret_cluster_point_map) {
        if (points_set.length == 1) {
          retained_list.append(points_set(0))
        }
        else {
          compression_list.append(new Cluster(points_set))
        }
      }
    }

    var i = 1

    while (i < 5) {
      // step 7. load next batch of data.
      train_data = shuffled_input_data.slice(random_chunk_size.toInt * i, (i + 1) * random_chunk_size.toInt)

      // steps 8, 9 and 10.

      for (data_point <- train_data) {
        var add_to_discard_list = false
        var add_to_compression_list = false
        var maha_dist = 0.0
        var min_maha_dist = Double.MaxValue
        var min_cluster : Cluster = null
        val feature_data = data_point.slice(2, data_point.length)
        for (cluster_obj <- discard_list) {
          maha_dist = calculate_mahalanobis_distance(feature_data, cluster_obj)
          if (maha_dist < 2 * math.sqrt(feature_data.length) && maha_dist < min_maha_dist) {
            min_maha_dist = maha_dist
            min_cluster = cluster_obj
            add_to_discard_list = true
          }
        }
        if (add_to_discard_list) {
          min_cluster.add_point(data_point)
        }
        else {
          for (cluster_obj <- compression_list) {
            maha_dist = calculate_mahalanobis_distance(feature_data, cluster_obj)
            if (maha_dist < 2 * math.sqrt(feature_data.length) && maha_dist < min_maha_dist) {
              min_maha_dist = maha_dist
              min_cluster = cluster_obj
              add_to_compression_list = true
            }
          }
          if (add_to_compression_list) {
            min_cluster.add_point(data_point)
          }
          else {
            retained_list.append(data_point)
          }
        }
      }

      // step 11. run K means on RS to generate RS and CS.
      val ret_saved_list = retained_list
      if (input_clusters * 10 < ret_saved_list.length) {
        val ret_K_means_results = perform_K_Means(ret_saved_list, sc, input_clusters * 10)
        val ret_pred_length = ret_K_means_results.length
        var ret_cluster_point_map = cluster_points_map(retained_list, ret_K_means_results, ret_pred_length)
        retained_list.clear()
        for ((_, points_set) <- ret_cluster_point_map) {
          if (points_set.length == 1) {
            retained_list.append(points_set(0))
          }
          else {
            compression_list.append(new Cluster(points_set))
          }
        }
      }
      // step 12. Merge CS clusters with maha_dist < 2 * sqrt(d)

      var seen_compression_clusters = ListBuffer[Cluster]()
      for (clusters <- compression_list.toSeq.combinations(2)) {
        if (!seen_compression_clusters.contains(clusters(0)) && !seen_compression_clusters.contains(clusters(1))) {
          val maha_dist = calculate_mahalanobis_distance_between_clusters(clusters(0), clusters(1))
          if (maha_dist < 2 * math.sqrt(clusters(0).centroid.length)) {
            val new_points_list = (clusters(0).pnts_list.toList ++ clusters(1).pnts_list.toList).to[ListBuffer]
            println(compression_list.length)
            compression_list -= clusters(0)
            compression_list -= clusters(1)
            println(compression_list.length)
            seen_compression_clusters.append(clusters(0))
            seen_compression_clusters.append(clusters(1))
            compression_list.append(new Cluster(new_points_list))
          }
        }
      }
      var discard_num_points = 0
      var compression_num_points = 0
      for (cluster <- discard_list) {
        discard_num_points += cluster.n
      }
      for (cluster <- compression_list) {
        compression_num_points += cluster.n
      }
      val s = Array(discard_num_points.toString, compression_list.length.toString, compression_num_points.toString, retained_list.length.toString).mkString(",")
      writer.write("Round " + (i + 1).toString + ": " + s + "\n")
      i += 1
    }
    // Last step : Merge CS and DS clusters.

    var seen_discard_clusters = ListBuffer[Cluster]()
    var seen_compression_clusters = ListBuffer[Cluster]()
    for (c_cluster <- compression_list) {
      for (d_cluster <- discard_list) {
        if (!seen_compression_clusters.contains(c_cluster) && !seen_discard_clusters.contains(d_cluster)) {
          val maha_dist = calculate_mahalanobis_distance_between_clusters(c_cluster, d_cluster)
          if (maha_dist < 2 * math.sqrt(c_cluster.centroid.length)) {
            val new_points_list = (c_cluster.pnts_list.toList ++ d_cluster.pnts_list.toList).to[ListBuffer]
            seen_compression_clusters.append(c_cluster)
            seen_discard_clusters.append(d_cluster)
            discard_list.append(new Cluster(new_points_list))
          }
        }
      }
    }
    for (cluster <- seen_discard_clusters) {
      discard_list -= cluster
    }
    for (cluster <- seen_compression_clusters) {
      compression_list -= cluster
    }
    println("FINISHED")
    var points_cluster_idx = ListBuffer[(Integer, Integer)]()
    for (i <- 0 until discard_list.length) {
      val points_list = discard_list(i).pnts_list
      for (point <- points_list) {
        points_cluster_idx.append((point(0).toInt, i))
      }
    }
    for (i <- 0 until compression_list.length) {
      val points_list = compression_list(i).pnts_list
      for (point <- points_list) {
        points_cluster_idx.append((point(0).toInt, -1))
      }
    }
    for (i <- 0 until retained_list.length) {
      points_cluster_idx.append((retained_list(i)(0).toInt, -1))
    }

    writer.write("\n" + "The clustering results:" + "\n")
    val sorted_points_cluster_idx = points_cluster_idx.sorted
    println("WRITING")
    writer.write(sorted_points_cluster_idx.map(x => x._1 + "," + x._2).mkString("\n"))
    //    for (i <- 0 until sorted_points_cluster_idx.length){
    //      val s = Array(sorted_points_cluster_idx(i)._1.toString,sorted_points_cluster_idx(i)._2.toString).mkString(",")
    //      writer.write(s + "\n")
  //}
    writer.close()
  }
}