import java.io.{File, PrintWriter}
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.math.BigDecimal
import scala.util.control.Breaks.{break, breakable}
import scala.math.max

object task2 extends Serializable {
  val prime_number     = 999999991
  val modulo_num       = 1000
  val hash_func_num    = 100
  val hash_functions_list = create_hash_functions(prime_number,modulo_num)

  class BlackBox {
    private val r1 = scala.util.Random

    def ask(filename: String, num: Int): Array[String] = {
      val input_file_path = filename

      val lines = Source.fromFile(input_file_path).getLines().toArray
      var stream = new Array[String](num)

      for (i <- 0 to num - 1) {
        stream(i) = lines(r1.nextInt(lines.length))
      }
      return stream
    }
  }

  def create_hash_functions(prime_number : Int, modulo_num : Int): ListBuffer[BigDecimal => BigDecimal] = {

    def generate_hash_functions(prime_number : Int, modulo_num : Int): Function1[BigDecimal, BigDecimal] = {
      val rand = scala.util.Random
      val a        = rand.nextInt().abs
      val b        = rand.nextInt().abs

      return (x : BigDecimal) => ((a * x + b)%prime_number)%modulo_num
    }
    val hash_functions_list = ListBuffer[Function1[BigDecimal, BigDecimal]]()
    for (i <- 0 until hash_func_num){
      val hash_func = generate_hash_functions(prime_number, modulo_num)
      hash_functions_list.append(hash_func)
    }
    return hash_functions_list
  }

//  def find_hash_values(user : String, hash_functions_list : ListBuffer[BigDecimal => BigDecimal]) : ListBuffer[BigDecimal]= {
//    //    println("USER")
//    //    println(user)
//    var hexed_user = ""
//    for (c <- user) {
//      hexed_user = hexed_user.concat(c.toInt.toString)
//    }
//    //      println("HEXED_USER")
//    //      println(hexed_user)
//    val hexed_bigd_user = BigDecimal(hexed_user)
//    var hash_list = ListBuffer[BigDecimal]()
//    for (hash_func <- hash_functions_list) {
//      hash_list.append(hash_func(hexed_bigd_user))
//    }
//    hash_list
//  }

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

  def find_trailing_zeros(hash_value : Int):Int={

    val binary_hash_value = hash_value.toBinaryString
    var trailing_zeros    = 0
    breakable{
    for (char <- binary_hash_value.reverse){
      if (char.toString.equals("0")){
        trailing_zeros += 1
      }
      else{
        break
      }
    }
    }
    trailing_zeros
  }

  def find_median(fm_number_list : ListBuffer[Double]):Int={
    val sorted_fm_list = fm_number_list.sorted
    var median = 0.0
    if (sorted_fm_list.size % 2 == 1){
      median = sorted_fm_list(sorted_fm_list.size / 2)
    }
    else{
      val (ceil, floor) = sorted_fm_list.splitAt(sorted_fm_list.size / 2)
      median = (ceil.last + floor.head)/2.0
    }
    median.toInt
  }

  def main(args: Array[String]): Unit = {
    val input_file  = args(0).toString
    val stream_size = args(1).toInt
    val num_asks    = args(2).toInt
    val output_file = args(3).toString
    var result      = ListBuffer[(Int, Int, Int)]()
    val bx = new BlackBox()
    for (i <- 0 until num_asks){
      var fm_number_list   = ListBuffer[Int]()
      var max_trail_zeros: Array[Int] = Array.fill[Int](hash_func_num)(0)
      val stream_users = bx.ask(input_file, stream_size)
      for (user <- stream_users){
        val hash_list = myhashs(user)
        for (j <- 0 until hash_list.length){
          hash_list(j) = find_trailing_zeros(hash_list(j).toInt)
          max_trail_zeros(j) = max_trail_zeros(j).max(hash_list(j).toInt)
        }
      }
      for (k <- max_trail_zeros){
        fm_number_list.append(math.pow(2, k).toInt)
      }
      //println(fm_number_list)
      var fm_avg_number_list = ListBuffer[Double]()
      for (z <- Range(0, hash_func_num, 10)){
        fm_avg_number_list.append(fm_number_list.slice(z, z+10).toList.sum/10.0)
      }
      println(fm_avg_number_list)
      val fm_number = find_median(fm_avg_number_list)
      val original_number = stream_users.length
      result.append((i, original_number, fm_number))
    }
    var a_sum = 0
    var e_sum = 0
    for (r <- result){
      a_sum += r._2
      e_sum += r._3
    }
    println(e_sum.toDouble / a_sum)

    val writer           = new PrintWriter( new File(output_file))
    writer.write("Time,Ground Truth,Estimation" + "\n")
    for (data <- result){
      writer.write(Array(data._1, data._2, data._3).mkString(",") + "\n")
    }
    writer.close()

  }
}