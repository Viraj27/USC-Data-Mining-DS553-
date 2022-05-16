
import sun.font.TrueTypeFont

import scala.io.Source
import scala.util
import java.io.{File, PrintWriter}
import scala.collection.Map
import scala.collection.mutable.{ListBuffer, Set => MutableSet}
import scala.util.control.Breaks.{break, breakable}
import scala.math.BigDecimal

object task1 extends Serializable {

  val prime_number     = 999999991
  var filter_bit_array: Array[Int] = Array.fill[Int](69997)(0)
  val len_array_bits: Int = filter_bit_array.length
  val previously_seen_user_set: MutableSet[String] = MutableSet[String]()
  val hash_func_num: Int = 5
  val hash_functions_list = create_hash_functions(prime_number,len_array_bits)

class BlackBox{
  private val r1 = scala.util.Random


  def ask(filename : String, num: Int): Array[String] = {
    val input_file_path = filename

    val lines = Source.fromFile(input_file_path).getLines().toArray
    var stream = new Array[String](num)

    for (i <- 0 to num - 1){
      stream(i) = lines(r1.nextInt(lines.length))
    }
    return stream
  }
}

  def create_hash_functions(prime_number : Int, len_array_bits : Int): ListBuffer[BigDecimal => BigDecimal] = {

    def generate_hash_functions(prime_number : Int, len_array_bits : Int): Function1[BigDecimal, BigDecimal] = {
      val rand = scala.util.Random
      val a        = rand.nextInt().abs
      val b        = rand.nextInt().abs

      return (x : BigDecimal) => ((a * x + b)%prime_number)%len_array_bits
    }
    val hash_functions_list = ListBuffer[Function1[BigDecimal, BigDecimal]]()
    for (i <- 0 until hash_func_num){
      val hash_func = generate_hash_functions(prime_number, len_array_bits)
      hash_functions_list.append(hash_func)
    }
    return hash_functions_list
  }

//  def find_hash_values(user : String, hash_functions_list : ListBuffer[BigDecimal => BigDecimal]) : ListBuffer[BigDecimal]= {
////    println("USER")
////    println(user)
//    var hexed_user = ""
//    for (c <- user) {
//        hexed_user = hexed_user.concat(c.toInt.toString)
//      }
////      println("HEXED_USER")
////      println(hexed_user)
//    val hexed_bigd_user = BigDecimal(hexed_user)
//    var hash_list = ListBuffer[BigDecimal]()
//    for (hash_func <- hash_functions_list) {
//        hash_list.append(hash_func(hexed_bigd_user))
//      }
//    hash_list
//    }

  def myhashs(user : String) : ListBuffer[BigDecimal]={
    var hexed_user = ""
    var hash_list  = ListBuffer[BigDecimal]()
    for (c <- user){
      hexed_user = hexed_user.concat(c.toInt.toString)
    }
    val hexed_bigd_user = BigDecimal(hexed_user)
    for (hash_func <- hash_functions_list){
      hash_list.append(hash_func(hexed_bigd_user))
    }
    hash_list
  }

  def bloom_filter(user : String, true_negative_users : ListBuffer[String], false_positive_users : ListBuffer[String]):Unit={
    val hash_list = myhashs(user)
    var user_seen_flag = true
    breakable{
    for (idx <- hash_list) {
      if (filter_bit_array(idx.toInt) == 0) {
        user_seen_flag = false
        break
      }
    }
    }
      if(! previously_seen_user_set.contains(user)){
        if(user_seen_flag){
        false_positive_users.append(user)
      }
        else{
          true_negative_users.append(user)
        }
    }
    previously_seen_user_set.add(user)
    for (idx <- hash_list){
      filter_bit_array(idx.toInt) = 1
    }
  }


  def main(args: Array[String]): Unit = {
    val input_file       = args(0).toString
    val stream_size      = args(1).toInt
    val num_asks         = args(2).toInt
    val output_file      = args(3).toString
    var len_dataset      = stream_size * num_asks
    val false_positive_data = ListBuffer[(Int, Double)]()
    val bx = new BlackBox()
    for (i <- 0 until num_asks){
      val stream_users         = bx.ask(input_file, stream_size)
      var true_negative_users  = ListBuffer[String]()
      var false_positive_users = ListBuffer[String]()
      for (user <- stream_users){
        bloom_filter(user, true_negative_users, false_positive_users)
      }
      val false_positive_rate = false_positive_users.length.toDouble / (false_positive_users.length + true_negative_users.length)
      false_positive_data.append((i, false_positive_rate))
//      for (user <- stream_users){
//        previously_seen_user_set.add(user)
//        val hash_list = myhashs(user)
//        for (idx <- hash_list){
//          filter_bit_array(idx.toInt) = 1
//        }
//        println("USER_SET")
//        println(previously_seen_user_set.size)
//        println("1's COUNT")
//        println(filter_bit_array.count(_ == 1))
      }
    val writer           = new PrintWriter( new File(output_file))
    writer.write("Time,FPR" + "\n")
    for (data <- false_positive_data){
      writer.write(Array(data._1, data._2).mkString(",") + "\n")
    }
    writer.close()
  }
}
