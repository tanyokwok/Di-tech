package ditech.time_slices

import com.houjp.common.io.IO
import com.houjp.ditech16
import ditech.common.util.DateIncrement

/**
  * Created by Administrator on 2016/5/27.
  */
object TrainTimeSlices {

//  val corr_slices_m2 = Array[Int](46,58,70,82,94,106,118,130,142).map(_ - 2)
//  val corr_slices_m1 = Array[Int](46,58,70,82,94,106,118,130,142).map(_ - 1)
//  val corr_slices_m = Array[Int](46,58,70,82,94,106,118,130,142)
//  val corr_slices_p1 = Array[Int](46,58,70,82,94,106,118,130,142).map(_ + 1)
//  val corr_slices_p2 = Array[Int](46,58,70,82,94,106,118,130,142).map(_ + 2)

//  val slices = corr_slices_m2 ++ corr_slices_m1 ++ corr_slices_m ++ corr_slices_p1 ++ corr_slices_p2
//  val corr_slices = Range(1,5).flatMap{
//  x =>
//    Array[Integer](46,58,70,82,94,106,118,130,142)
//  }
//  val slices = Range(1,145).toArray ++ corr_slices
   val slices = Range(1,145).toArray

//  val slices = Array[Integer](46,58,70,82,94,106,118,130,142)
//  val slices = Array[Integer](45,46,47,57,58,59,69,70,71,81,82,83,93,94,95,105,106,107,117,118,119,129,130,131,141,142,143)
  def run(output_pt:String,start_date:String,dayCount:Int): Unit ={
    val date = DateIncrement(start_date)
    val list: Array[String] = Range(0,dayCount).map{
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

  def genTestSlices( output_pt:String): Unit ={
    val slices = Array[Integer](46,58,70,82,94,106,118,130,142)
    val dates = Array[String]("2016-01-15","2016-01-16","2016-01-17","2016-01-18","2016-01-20")
    val list: Array[(String, String, String)] = dates.map{
      date =>
        val date_str = date.toString
        slices.map{
          slice =>
            ( s"$date_str-${slice-1}", s"$date_str-$slice", s"$date_str-${slice+1}" )
        }
    }.reduce( _ ++ _ )

    IO.write(output_pt + "/val_time_slices1",list.map( _._1 ) )
    IO.write(output_pt + "/test_time_slices",list.map( _._2 ) )
    IO.write(output_pt + "/val_time_slices2",list.map( _._3 ) )
  }
  def main(args:Array[String]): Unit ={
//    val date = DateIncrement("2016-01-08")
    val train_pt = ditech16.train_pt  + "/train_time_slices"
    val test_pt = ditech16.test1_pt + "/train_time_slices"

    genTestSlices(ditech16.train_pt)
//    run(train_pt,"2016-01-01",14)
    run(test_pt,"2016-01-01",21)
//
  }
}
