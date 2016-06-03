package ditech.time_slices

import ditech.common.util.DateIncrement
import com.houjp.common.io.IO

import scala.collection.immutable.IndexedSeq

/**
  * Created by Administrator on 2016/5/27.
  */
object TrainTimeSlices {

  val slices = Array[Integer](46,58,70,82,94,106,118,130,142)

  def main(args:Array[String]): Unit ={
    val date = DateIncrement("2016-01-08")
//    val date = DateIncrement("2016-01-01")
    val output_pt = ditech.train_pt  + "/train_time_slices"
    val list: Array[String] = Range(0,7).map{
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
}
