package com.houjp.ditech16.feature

import java.text.SimpleDateFormat
import java.util.Calendar

import com.houjp.common.Log
import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{OrderAbs, District}
import ditech.common.util.Directory
import org.saddle.Vec

object FDTDemand extends Log {

  val districts_fp = ditech16.data_pt + "/cluster_map/cluster_map"
  val districtsType = District.loadDidTypeId(districts_fp)
  val districts = districtsType.mapValues( _._1 )

  def main(args:Array[String]): Unit ={
    run(ditech16.data_pt,FDTDemand.getClass.getSimpleName.replace("$",""))
  }


  val stat_map = getStatisticsByDate()

  def getStatisticsByDate() ={
    val dates_arr = IO.load(ditech16.data_pt + "/overview_dates").map{
      line =>
        val Array(date,type_s) = line.split("\t")
        (date, type_s.toInt)
    }

    val gaps_map = collection.mutable.Map[(Int,Int),Array[Double]]()

    dates_arr.foreach{
     case (now_date, type_id)=>

       val now_orders = OrderAbs.load_local( ditech16.data_pt + s"/order_abs_data/order_data_$now_date" )

       val fs = collection.mutable.Map[(Int, Int), Double]()

       val date_formate = new SimpleDateFormat("yyyy-MM-dd")
       val cal = Calendar.getInstance()
       cal.setTime(date_formate.parse(now_date))
       cal.set(Calendar.DATE, cal.get(Calendar.DATE) + 1)
       val aft_date = date_formate.format(cal.getTime)

       val aft_orders = OrderAbs.load_local(ditech16.data_pt + s"/order_abs_data/order_data_$aft_date")

       log(s"aft_date($aft_date),now_date($now_date)")

       aft_orders.foreach { e =>
         val s_ntid = e.new_time_id - 9
         val e_ntid = e.new_time_id - 0

         if (-1 != e.start_district_id) {
           Range(s_ntid + 1440, 1440 + 1).foreach { ntid =>
             fs((e.start_district_id, ntid)) = fs.getOrElse((e.start_district_id, ntid), 0.0) + 1.0
           }
         }
       }

       now_orders.foreach { e =>
         val s_ntid = e.new_time_id - 9
         val e_ntid = e.new_time_id - 0

         if (-1 != e.start_district_id) {
           Range(math.max(1, s_ntid), e_ntid + 1).foreach { ntid =>
             fs((e.start_district_id, ntid)) = fs.getOrElse((e.start_district_id, ntid), 0.0) + 1.0
           }
         }
       }

       districtsType.values.toArray.filter{
          case (did, tp) =>
            tp == type_id || tp == 0
        }.foreach{
          case (did,tp)=>
            Range(1,ditech16.max_new_time_id + 1  ).foreach{
              ntid =>
                gaps_map( (did,ntid) ) = gaps_map.getOrElse((did,ntid), Array[Double]()) ++ Array(fs.getOrElse((did,ntid),0.0))
            }
        }
    }
    gaps_map.mapValues{
      fs =>
        val fs_vec = Vec( fs )
        (fs_vec.mean, fs_vec.median, fs_vec.stdev,fs_vec.min.getOrElse(0.0), fs_vec.max.getOrElse(0.0))
    }
  }

 def run( data_pt:String, feat_name:String ): Unit ={
   val date_fp = data_pt + "/dates"
   val dates = IO.load(date_fp).distinct
   dates.foreach{
      date =>
        val feat_dir = data_pt + s"/feature/$feat_name"
        Directory.create( feat_dir)
        val feat_fp = feat_dir + s"/${feat_name}_$date"

        val feats = districts.values.toArray.sorted.flatMap { did =>
          Range(1, 1440 + 1).map {
            tid =>
              val f = stat_map.getOrElse((did,tid),(0,0,0,0,0))
              s"$did,$tid\t${f._1},${f._2},${f._3},${f._4},${f._5}"
//              val f = stat_map.getOrElse((did,tid),(0,0,0))
//              s"$did,$tid\t${f._1},${f._2},${f._3}"
          }
        }

        IO.write(feat_fp, feats )
    }
  }

}
