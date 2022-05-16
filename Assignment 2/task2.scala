import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, PrintWriter}
import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}
import scala.collection.mutable.{Set => MutableSet}
import scala.math.Ordering.Implicits._
import scala.collection.mutable.{ListBuffer, Set => MutableSet}

object task2 extends Serializable{

  def find_candidate_itemsets(freq_item_sets: ListBuffer[Set[String]]) : ListBuffer[Set[String]] = {
    val res: MutableSet[Set[String]] = MutableSet()
    val candidate_item_set_length = freq_item_sets.head.size + 1

    val n = freq_item_sets.length
    for (i <- 0 to n - 2){
      for (j <- i + 1 until n){
        val new_item_set = freq_item_sets(i).union(freq_item_sets(j))
        if (new_item_set.size == candidate_item_set_length){
          res += new_item_set
        }
      }
    }
    res.to[mutable.ListBuffer]
  }

  def find_frequent_itemset(candidate_item_sets: mutable.ListBuffer[Set[String]], baskets: Array[Set[String]], partial_support: Float): mutable.ListBuffer[Set[String]] = {
    val res = mutable.ListBuffer[Set[String]]()
    for (item_set <- candidate_item_sets){
      var counter = 0
      for (b <- baskets){
        if (item_set.subsetOf(b)){
          counter = counter + 1
        }
      }

      if (counter >= partial_support){
        res += item_set
      }
    }
    res
  }

  def find_frequent_itemsets(baskets: Array[Set[String]], support: Int, totalBaskets: Long): Iterator[Set[String]] = {
    var res            = mutable.ListBuffer[Set[String]]()
    val count_map      = mutable.HashMap[String, Int]()
    var freq_item_sets = mutable.ListBuffer[Set[String]]()

    for (b <- baskets) {
      for (i <- b) {
        count_map(i) = count_map.getOrElse(i, 0) + 1
      }
    }

    val partial_support = (support.toFloat * baskets.length) / totalBaskets
    for ((k, v) <- count_map){
      if (v >= partial_support){
        freq_item_sets += Set(k)
        res += Set(k)
      }
    }

    breakable {
      while(freq_item_sets.nonEmpty){
        val candidate_item_sets = find_candidate_itemsets(freq_item_sets)
        if(candidate_item_sets.nonEmpty){
          freq_item_sets = find_frequent_itemset(candidate_item_sets, baskets, partial_support)
          if(freq_item_sets.nonEmpty){
            // concatenate
            res = res ++ freq_item_sets
          }
        }else{
          // while loop break condition
          break
        }
      }
    }

    res.toIterator
  }

  def get_item_count_per_candidate(baskets: Array[Set[String]], candidate_sets: Array[Set[String]]): Iterator[(Set[String], Long)] = {
    val res = mutable.ListBuffer[(Set[String], Long)]()
    for(item_sets <- candidate_sets){
      var cnt = 0
      for(b <- baskets){
        if(item_sets.subsetOf(b)){
          cnt = cnt + 1
        }
      }
      res += Tuple2(item_sets, cnt)
    }

    res.toIterator
  }


  def format_output(item_sets: Array[Set[String]], writer: PrintWriter) :Unit = {
    val sorted_grouped_item_sets = item_sets.groupBy(l => l.size).toList.sortBy(_._1)
    for ((_, item_sets) <- sorted_grouped_item_sets){
      val sorted_item_sets = item_sets.map(item_set => item_set.toList.sorted).sorted.map(item_set => "(" + item_set.map(i => "'" + i + "'").mkString(", ") + ")").mkString(",")
      writer.write(sorted_item_sets + "\n\n")
    }
  }

  def write_output(candidate_sets: Array[Set[String]], frequent_sets: Array[Set[String]], output_file: String): Unit = {

    val writer = new PrintWriter(new File(output_file))
    writer.write("Candidates:\n")
    format_output(candidate_sets, writer)
    writer.write("Frequent Itemsets:\n")
    format_output(frequent_sets, writer)
    writer.close()
  }

  def task2_son(filter_threshold: Int, support: Int, input_file : String, output_file: String, sc : SparkContext) : Unit = {

    var input_data_rdd = sc.textFile(input_file).map(l => l.split(','))
    val header         = input_data_rdd.first()
    input_data_rdd     = input_data_rdd.filter(l => l != header)
    // Basket Creation
    // Combinations of frequent businesses
    // initialize baskets with entire rdd and then create baskets
    var baskets =  input_data_rdd.map(l => (l(0), l(1)))
    var buckets = baskets.groupByKey().map(l => l._2.toSet)
    buckets     = buckets.filter(l => l.size > filter_threshold)
    val totalBaskets = buckets.count()
    val candidate_sets = buckets.mapPartitions(l => find_frequent_itemsets(l.toArray, support, totalBaskets)).distinct().collect()
    val frequent_sets = buckets.mapPartitions(l => get_item_count_per_candidate(l.toArray, candidate_sets)).reduceByKey((a,b)=> a+b).filter(l=>l._2 >= support).map(l=>l._1).collect()

    write_output(candidate_sets, frequent_sets, output_file)

  }


  def format_csv_dataset(sc: SparkContext, input_file: String, formatted_csv_file: String): Unit = {
    var input_data_rdd = sc.textFile(input_file)
    val header_data = input_data_rdd.first()
    input_data_rdd = input_data_rdd.filter(l => l != header_data)
    val formatted_data = input_data_rdd.map(data => (data.split(",")(0).stripPrefix("\"").stripSuffix("\"") + "-" + data.split(",")(1).stripPrefix("\"").stripSuffix("\""), data.split(",")(5).stripPrefix("\"").stripSuffix("\"").toLong)).collect()

    val writer = new PrintWriter(new File(formatted_csv_file))
    writer.write("DATE-CUSTOMER_ID,PRODUCT_ID\n")
    for(row <- formatted_data) {
      writer.write(row._1 + "," + row._2 + "\r\n")
    }
    writer.close()
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DM_assign2_task2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val filter_threshold = args(0).toInt
    val support = args(1).toInt
    val input_file = args(2)
    val output_file = args(3)
    val formatted_csv_file = "customer_product_file.csv"
    format_csv_dataset(sc, input_file, formatted_csv_file)
    val startTime      = System.nanoTime()
    task2_son(filter_threshold, support, formatted_csv_file, output_file ,sc)
    println("Duration: " + (System.nanoTime - startTime) / 1e9d)
  }
}
