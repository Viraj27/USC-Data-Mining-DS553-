import scala.io.Source
import java.io.{File, PrintWriter, FileWriter}
object task3 extends Serializable {

  var fixed_size_sampling_list: Array[String] = Array.fill[String](100)("")
  var user_sequence_number = 0

  class BlackBox{
    private val r1 = scala.util.Random
    r1.setSeed(553)
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
  def main(args: Array[String]): Unit = {
    val r2               = scala.util.Random
    r2.setSeed(553)
    val input_file       = args(0).toString
    val stream_size      = args(1).toInt
    val num_asks         = args(2).toInt
    val output_file      = args(3).toString
    var i                = 0
    val bx               = new BlackBox()
    val writer           = new PrintWriter( new File(output_file))
    //write header to csv
    writer.write("seqnum,0_id,20_id,40_id,60_id,80_id"+"\n")

    for (_ <- 0 until num_asks){
      var stream_users = bx.ask(input_file, stream_size)
      for (user <- stream_users) {
        user_sequence_number += 1
      if (user_sequence_number > fixed_size_sampling_list.length){
          if ((stream_users.length.toDouble / user_sequence_number)>r2.nextFloat()){
            val new_idx = r2.nextInt(100)
            fixed_size_sampling_list(new_idx) = user
          }
        }
        else{
        fixed_size_sampling_list(i) = user
        i += 1
      }
      }
      writer.write(Array(user_sequence_number, fixed_size_sampling_list(0), fixed_size_sampling_list(20), fixed_size_sampling_list(40), fixed_size_sampling_list(60), fixed_size_sampling_list(80)).mkString(",")+"\n" )
    }
    writer.close()
  }

}
