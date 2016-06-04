package ditech.time_slices

import com.houjp.common.io.IO
import ditech.common.util.DateIncrement

/**
  * Created by Administrator on 2016/5/27.
  */
object TrainTimeSlices {

//  val slices = Range(36,144).toArray
//  val slices = Array[Integer](46,58,70,82,94,106,118,130,142)
  val slices = Array[Integer](45,46,47,57,58,59,69,70,71,81,82,83,93,94,95,105,106,107,117,118,119,129,130,131,141,142,143)
  def run(output_pt:String,start_date:String): Unit ={
    val date = DateIncrement(start_date)
    val list: Array[String] = Range(0,14).map{
      x =>
        val date_str = date.toString
        date.next()
        slices.map{
          slice =>
             s"$date_str-$slice"
        }
    }.reduce( _ ++ _ )

    IO.write(output_pt, list)
  }

  def main(args:Array[String]): Unit ={
//    val date = DateIncrement("2016-01-08")
    val train_pt = ditech.train_pt  + "/train_time_slices"
    val test_pt = ditech.test1_pt + "/train_time_slices"

    run(train_pt,"2016-01-01")
    run(test_pt,"2016-01-08")
  }
}
