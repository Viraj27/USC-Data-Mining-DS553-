import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import java.io.{File, PrintWriter}
import scala.collection.mutable.{ListBuffer, Set => MutableSet}
import scala.collection.{Map, mutable}
import scala.collection.mutable._
import scala.util.control.Breaks.{break, breakable}



object task2 extends Serializable{

  class User_Node (usr_id : String, parent_set : MutableSet[User_Node], children_set : MutableSet[User_Node], level_from_root : Int, shortest_paths_from_root : Int, credits : Int) extends Serializable {
    var user_id : String = usr_id
    var parents_set : MutableSet[User_Node] = parent_set
    var childrens_set : MutableSet[User_Node] = children_set
    var levels_from_root : Int = level_from_root
    var shortest_path_from_root : Int = shortest_paths_from_root
    var credit : Double = credits

  }


  def find_neighbors_set(row : (String, Set[String]), user_bid_map : Map[String, Set[String]], filter_threshold : Int): (String, MutableSet[String])={
    var neighbors_set = MutableSet[String]()
    val user_id = row._1
    val business_set = row._2
    for ((potential_neighbor, their_business_set) <- user_bid_map){
      val intersection = business_set.intersect(their_business_set)
      if (user_id != potential_neighbor && intersection.size >= filter_threshold){
        neighbors_set.add(potential_neighbor)

      }

    }
    (user_id, neighbors_set)
  }

  def generate_bfs_tree(row: (String, MutableSet[String]), neighbors_set_data: Map[String, MutableSet[String]]):(String, mutable.Map[String, User_Node])={

    val user_id = row._1
    val user_node = new User_Node(user_id, MutableSet(), MutableSet(), 0, 1, 0)

    val queue = ListBuffer[String]()
    var visited = mutable.Map[String, User_Node]()
    queue.append(user_id)
    visited(user_id) = user_node
    var level = 1
    while (queue.length != 0){
      val level_size = queue.length
      for (_ <- 0 until level_size){
        val current_user_id = queue.remove(0)
        val current_user_node = visited.getOrElse(current_user_id, new User_Node(current_user_id, MutableSet(), MutableSet(), 0, 1, 0))
        val neighbors_set = neighbors_set_data.getOrElse(current_user_id, Set())
        for (neighbor <- neighbors_set){
          if( visited.contains(neighbor)){
            val neighbor_node = visited.getOrElse(neighbor, new User_Node(neighbor, MutableSet(), MutableSet(), 0, 1, 0))
            if (! current_user_node.parents_set.contains(neighbor_node) && current_user_node.levels_from_root != neighbor_node.levels_from_root){
              neighbor_node.parents_set.add(current_user_node)
              current_user_node.childrens_set.add(neighbor_node)
            }

          }
          else{
            val neighbor_node = new User_Node(neighbor, MutableSet(), MutableSet(), level, 1, 0)
            neighbor_node.parents_set.add(current_user_node)
            current_user_node.childrens_set.add(neighbor_node)
            queue.append(neighbor)
            visited(neighbor) = neighbor_node
          }
        }
      }
      level += 1

    }
    (user_id, visited)
  }

  def generate_shortest_paths_count(row : (String, mutable.Map[String, User_Node]))={
    val user_id = row._1
    val bfs_tree = row._2
    var levels_map = mutable.Map[Int, MutableSet[User_Node]]().withDefaultValue(MutableSet[User_Node]())
    var visited = MutableSet[String]()

    bfs_tree(user_id).shortest_path_from_root = 1
    var queue = ListBuffer[String]()
    queue.append(user_id)
    visited.add(user_id)

    while (queue.size != 0){
      val current_user_id = queue.remove(0)
      val current_user_node = bfs_tree.getOrElse(current_user_id, new User_Node(current_user_id, MutableSet(), MutableSet(), 0, 0, 0))
      val current_user_level = current_user_node.levels_from_root
      levels_map.update(current_user_level, levels_map(current_user_level)+current_user_node)
      if (current_user_node.parents_set.size == 0){
        current_user_node.shortest_path_from_root = 1
      }
      var num_shortest_paths = 0
      if (current_user_node.parents_set.size > 0){
        for (parent <- current_user_node.parents_set){
          num_shortest_paths += parent.shortest_path_from_root
        }
        current_user_node.shortest_path_from_root = num_shortest_paths
      }
      for (children <- current_user_node.childrens_set){
        if (!visited.contains(children.user_id)){
          queue.append(children.user_id)
          visited.add(children.user_id)
        }
      }
    }
    val sorted_levels = Map(levels_map.toSeq.sortWith(_._1 > _._1):_*) //remove
    (user_id, sorted_levels)
  }

  def is_leaf_node(node : User_Node):Boolean={
    if(node.childrens_set.size == 0){
      return true
    }
    else{
      return false
    }
  }

  def generate_credit_allocation(row : (String, Map[Int, MutableSet[User_Node]])):(String, mutable.Map[(String,String), Double])={

    var edge_credit_map = mutable.Map[(String, String), Double]()
    val user_id = row._1
    val levels_dict = row._2
    val levels = levels_dict.toArray.sortBy(x => -1 * x._1)
    for((_,nodes) <- levels){
      for (node <- nodes){

        if (is_leaf_node(node)){

          node.credit = 1.0
        }
        else{
          var sum_edges = 0.0
          for (child_node <- node.childrens_set){
            var edge = List(child_node.user_id, node.user_id).sorted
            sum_edges += edge_credit_map.getOrElse((edge(0), edge(1)), 0.0)
          }
          node.credit = 1.0 + sum_edges
        }
        if (node.parents_set.size > 0){
          var total_credits = 0.0
          for (p <- node.parents_set){
            total_credits += p.shortest_path_from_root
          }
          for (parent_node <- node.parents_set){
            var edge = List(parent_node.user_id, node.user_id).sorted
            edge_credit_map((edge(0), edge(1))) = (parent_node.shortest_path_from_root / total_credits) * node.credit
          }
        }
      }
    }

    (user_id, edge_credit_map)
  }

  def calculate_betweenness(credit_allocated_map : Map[String, mutable.Map[(String, String), Double]]):Array[((String, String),Double)]={

    var edge_betweenness_map = mutable.Map[(String, String), Double]().withDefaultValue(0.0)
    for ((_, edge_credit_map) <- credit_allocated_map){
      for ((edge, credit) <- edge_credit_map){
        edge_betweenness_map.update(edge, edge_betweenness_map(edge) + credit)
      }
    }
    var edge_betweenness_list = ListBuffer[((String, String), Double)]()
    for((edge, credit) <- edge_betweenness_map){
      edge_betweenness_list.append((edge, math.round((credit / 2.0) * 100000.0) / 100000.0))
    }
    var sorted_edge_betweenness_list = edge_betweenness_list.toIterator.toArray.sortBy(x => (-x._2, x._1))

    sorted_edge_betweenness_list
  }

  def beetweenness_finder(neighbors_set_rdd : RDD[(String, MutableSet[String])], neighbors_set_data : Map[String, MutableSet[String]]): Array[((String, String), Double)] ={
    var bfs_tree_rdd = neighbors_set_rdd.map(row => generate_bfs_tree(row, neighbors_set_data))
    var shortest_path_rdd = bfs_tree_rdd.map(row => generate_shortest_paths_count(row))
    var credit_allocated_rdd = shortest_path_rdd.map(row => generate_credit_allocation(row))
    val first_entry = credit_allocated_rdd.first()
    var credit_allocated_map = credit_allocated_rdd.collectAsMap()
//    val writer3 = new PrintWriter(new File("credit_vals.txt"))
//    for (r <- credit_allocated_rdd.collect()){
//      writer3.write(r.toString())
//    }
//    writer3.close()
//    for((k,v) <- credit_allocated_map) {
//      for((a,b) <- v){
//        println(k.toString + (a.toString(), b.toString).toString())
//      }
//    }
    calculate_betweenness(credit_allocated_map)
  }

  def detect_community_in_graph(node : String, graph : Map[String, mutable.Set[String]], connected_components : MutableSet[String]):MutableSet[String]={
    var visited_nodes = MutableSet[String]()
    visited_nodes.add(node)
    var queue = ListBuffer[String]()
    queue.append(node)
    while(queue.length !=0){
      val level_size = queue.length
      for (_ <- 0 until level_size){
        val current_node = queue.remove(0)
        for (neighbor <- graph(current_node)){
          if (!visited_nodes.contains(neighbor)){
            visited_nodes.add(neighbor)
            connected_components.add(neighbor)
            queue.append(neighbor)
          }
        }
      }
    }
    visited_nodes
  }

  def communities_in_graph(graph: Map[String, mutable.Set[String]]):ListBuffer[Array[String]]={
    val connected_components = MutableSet[String]()
    var communities = ListBuffer[Array[String]]()
    var community = MutableSet[String]()

    for ((node,_) <- graph){
      if (!connected_components.contains(node)){
        connected_components.add(node)
        community = detect_community_in_graph(node, graph, connected_components)
        communities.append(community.toArray)
      }
    }
    communities
  }

  def evaluate_Q_score(A:Map[String, MutableSet[String]], m:Int, communities:ListBuffer[Array[String]]):Double={
    var q_score_list = ListBuffer[Double]()
    for (community <- communities){
      var q_score = 0.0
      for (node_1 <- community){
        for(node_2 <- community){
          var matrix_a_edge_value = 0.0
          if (A(node_1).contains(node_2)) {
            matrix_a_edge_value = 1.0
          }
          val node_1_score = A.getOrElse(node_1, MutableSet[String]()).size
          val node_2_score = A.getOrElse(node_2, MutableSet[String]()).size
          var expected_value = ((node_1_score * node_2_score) / (2.0 * m))
          q_score += ((matrix_a_edge_value - expected_value) / (2.0 * m))
        }
      }
      q_score_list.append(q_score)
    }
    var total_q_score = 0.0
    for (q_score <- q_score_list){
      total_q_score += q_score
    }
    total_q_score
  }

  def detect_suitable_communities(m : Int, heaviest_edge : ((String, String), Double), graph : Map[String, MutableSet[String]], A:Map[String, MutableSet[String]], sc : SparkContext):(ListBuffer[Array[String]], ListBuffer[Double])={
    val iteration_length = m
    var heavy_edge = heaviest_edge
    var communities = ListBuffer[Array[String]]()
    var most_suitable_communities = ListBuffer[Array[String]]()
    var max_q_score = -(Double.MaxValue)
    var q_score_list = ListBuffer[Double]()
      for (i <- 0 until iteration_length) {
        // remove edge : remove node2 from node1 set and node1 from node2 set.
        graph(heavy_edge._1._1).remove(heavy_edge._1._2)
        graph(heavy_edge._1._2).remove(heavy_edge._1._1)
        val communities = communities_in_graph(graph)
        val q_score = evaluate_Q_score(A, m, communities)
        q_score_list.append(q_score)
        if (q_score > max_q_score) {
          max_q_score = q_score
          most_suitable_communities = communities
        }

        if (i != m - 1) {
          val graph_array_for_rdd = graph.toArray
          val graph_rdd = sc.parallelize(graph_array_for_rdd)
          heavy_edge = beetweenness_finder(graph_rdd, graph)(0)
        }
      }
    (most_suitable_communities,q_score_list)
  }


  def task2(filter_threshold : Int, input_file : String, beetweenness_file : String, output_file : String, sc : SparkContext):Unit={
    var input_data_rdd = sc.textFile(input_file).map(row => row.split(','))
    val header         = input_data_rdd.first()
    var processed_rdd  = input_data_rdd.filter(row => row != header).map(row=>(row(0), Set(row(1)))).reduceByKey((row1, row2)=>row1 | row2)
    val user_bid_map   = processed_rdd.collectAsMap()

    // create graph
    val neighbors_set_rdd  = processed_rdd.map(row => find_neighbors_set(row, user_bid_map, filter_threshold)).filter(row=> row._2.size > 0)

    val neighbors_set_map = neighbors_set_rdd.collectAsMap()
    val graph             = neighbors_set_rdd.collectAsMap()
    val A                 = neighbors_set_rdd.collectAsMap()
    val clone_for_comm_finding = neighbors_set_rdd.collectAsMap()

    val edge_betweenness_list = beetweenness_finder(neighbors_set_rdd, neighbors_set_map)
    val last_edge_betweenness = edge_betweenness_list.last

    val writer = new PrintWriter(new File(beetweenness_file))
    for(edge_betweenness <- edge_betweenness_list) {
      writer.write("('" + edge_betweenness._1._1 + "', '" + edge_betweenness._1._2 + "')," + edge_betweenness._2 )
      if (edge_betweenness != last_edge_betweenness){
        writer.write('\n')
      }
    }
    writer.close()

    val m = edge_betweenness_list.length
    val heaviest_edge = edge_betweenness_list(0)
    var sorted_communities = ListBuffer[Array[String]]()
    val most_suitable_communities = detect_suitable_communities(m, heaviest_edge, graph, A, sc)
    println("MAX Q SCORE")
    println(most_suitable_communities._2)
    for (community <- most_suitable_communities._1){
        sorted_communities.append(community.sorted)
    }
    var final_sorted_communities = sorted_communities.sortBy(x => (x.length, x(0)))
    val writer2 = new PrintWriter(new File(output_file))
    for (community <- final_sorted_communities){
      writer2.write(community.map(x => "'" + x + "'").mkString(", ") + "\n")
    }
    writer2.close()
  }
  def main(args: Array[String]): Unit = {
    val startTime         = System.nanoTime()
    val conf = new SparkConf().setAppName("DM_assign4_task1").setMaster("local[*]")
    val spark = SparkSession.builder.appName("DM_assign4_task1").config(conf).getOrCreate()
    val sc    = spark.sparkContext
    sc.setLogLevel("ERROR")

    val filter_threshold  = args(0).toInt
    val input_file        = args(1).toString
    val beetweenness_file = args(2).toString
    val output_file       = args(3).toString
    task2(filter_threshold, input_file, beetweenness_file, output_file, sc)
    println("Duration: " + (System.nanoTime - startTime) / 1e9d)
  }
}
