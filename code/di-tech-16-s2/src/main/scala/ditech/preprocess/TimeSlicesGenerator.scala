package ditech.preprocess

import com.houjp.common.io.IO
import com.houjp.ditech16
import ditech.common.util.Directory

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2016/5/27.
  */
object TimeSlicesGenerator {

//  val corr_slices_m2 = Array[Int](46,58,70,82,94,106,118,130,142).map(_ - 2)
//  val corr_slices_m1 = Array[Int](46,58,70,82,94,106,118,130,142).map(_ - 1)
//  val corr_slices_m = Array[Int](46,58,70,82,94,106,118,130,142)
//  val corr_slices_p1 = Array[Int](46,58,70,82,94,106,118,130,142).map(_ + 1)
//  val corr_slices_p2 = Array[Int](46,58,70,82,94,106,118,130,142).map(_ + 2)

//  val slices = corr_slices_m2 ++ corr_slices_m1 ++ corr_slices_m ++ corr_slices_p1 ++ corr_slices_p2
//  val corr_slices = Range(0,1).flatMap{
//  x =>
//    Array[Integer](46,58,70,82,94,106,118,130,142)
//  }
//  val slices = Range(1,145).toArray ++ corr_slices
//  val slices = Array[Integer](46,58,70,82,94,106,118,130,142)
//  val slices = Array[Integer](45,46,47,57,58,59,69,70,71,81,82,83,93,94,95,105,106,107,117,118,119,129,130,131,141,142,143)
  def run(step:Int,output_pt:String, end_start:String): Unit ={

    val dates = IO.load(ditech16.data_pt + "/dates").filter( _ <= end_start ).distinct
    val list: Array[String] = dates.map{
      date_str =>
        var slice = 1
        val date_slices = new ArrayBuffer[String]()
        while( slice < ditech16.max_new_time_id + 1) {
          date_slices += s"$date_str-$slice"
          slice = slice + step
        }
        date_slices.toArray
    }.reduce( _ ++ _ )

    IO.write(output_pt, list)
  }

  def genTestSlices( output_pt:String): Unit ={
    val slices = Array[Integer](46,58,70,82,94,106,118,130,142).map{
      slice => (slice - 1)*10 + 1
    }
    val dates = Array[String]("2016-03-11","2016-03-12","2016-03-13","2016-03-14","2016-03-15", "2016-03-16", "2016-03-17")
    val list: Array[(String, String, String)] = dates.map{
      date =>
        val date_str = date.toString
        slices.map{
          slice =>
            ( s"$date_str-${slice-10}", s"$date_str-$slice", s"$date_str-${slice+10}" )
        }
    }.reduce( _ ++ _ )

    IO.write(output_pt + "/val_time_slices1",list.map( _._1 ) )
    IO.write(output_pt + "/test_time_slices",list.map( _._2 ) )
    IO.write(output_pt + "/val_time_slices2",list.map( _._3 ) )
  }
  def main(args:Array[String]): Unit ={
//    val date = DateIncrement("2016-01-08")
    Directory.create( args(1) + "/offline")
    Directory.create( args(1) + "/online")
    genTestSlices( ditech16.offline_pt)
    run(args(0).toInt,args(1) + "/offline/train_time_slices","2016-03-10")
    run(args(0).toInt,args(1) + "/online/train_time_slices","2016-03-17")
//
  }
}
