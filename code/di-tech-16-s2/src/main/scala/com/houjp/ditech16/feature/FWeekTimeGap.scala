package com.houjp.ditech16.feature

import java.text.SimpleDateFormat
import java.util.Calendar

import com.houjp.common.Log
import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{OrderAbs, District}
import ditech.common.util.Directory
import org.saddle.Vec

object FWeekTimeGap extends Log{

  val districts_fp = ditech16.data_pt + "/cluster_map/cluster_map"
  val districtsType = District.loadDidTypeId(districts_fp)
  val districts = districtsType.mapValues( _._1 )

  def main(args:Array[String]): Unit ={
    run(ditech16.data_pt,this.getClass.getSimpleName.replace("$",""))
  }

  def getFeatMap(dates:IndexedSeq[(String,Int,Int)]): collection.Map[Int,(Double, Double, Double, Double, Double)] ={
    val gaps_map = collection.mutable.Map[Int, Array[Double]]()
   //get gaps of every day
   dates.foreach{
     case (now_date,weekday,type_id)=>
       val now_orders = OrderAbs.load_local( ditech16.data_pt + s"/order_abs_data/order_data_$now_date" )

       val date_formate = new SimpleDateFormat("yyyy-MM-dd")
       val cal = Calendar.getInstance()
       cal.setTime(date_formate.parse(now_date))
       cal.set(Calendar.DATE, cal.get(Calendar.DATE) + 1)
       val aft_date = date_formate.format(cal.getTime)

       val aft_orders = OrderAbs.load_local(ditech16.data_pt + s"/order_abs_data/order_data_$aft_date")

       log(s"aft_date($aft_date),now_date($now_date)")

       val fs = collection.mutable.Map[Int, Double]()

       aft_orders.foreach { e =>
         val s_ntid = e.new_time_id - 9
         val e_ntid = e.new_time_id - 0

         if (-1 != e.start_district_id && !e.has_driver) {
           Range(s_ntid + 1440, 1440 + 1).foreach { ntid =>
             fs( ntid) = fs.getOrElse( ntid, 0.0) + 1.0
           }
         }
       }

       now_orders.foreach { e =>
         val s_ntid = e.new_time_id - 9
         val e_ntid = e.new_time_id - 0

         if (-1 != e.start_district_id && !e.has_driver) {
           Range(math.max(1, s_ntid), e_ntid + 1).foreach { ntid =>
             fs( ntid) = fs.getOrElse( ntid, 0.0) + 1.0
           }
         }
       }


       Range(1, ditech16.max_new_time_id + 1).foreach{
         ntid =>
          gaps_map( ntid) = gaps_map.getOrElse(ntid, Array[Double]()) ++ Array(fs.getOrElse(ntid,0.0))
       }
    }

    gaps_map.mapValues{
      fs =>
        val fs_vec = Vec( fs )
        (fs_vec.mean, fs_vec.median, fs_vec.stdev,fs_vec.min.getOrElse(0.0), fs_vec.max.getOrElse(0.0))
    }
  }

  val stat_map = getStatisticsByDate()
  def getStatisticsByDate() ={
    val dates_arr = IO.load(ditech16.data_pt + "/overview_dates").map{
      line =>
        val Array(date,type_s) = line.split("\t")
        (date, type_s.toInt)
    }

    val gaps_map = collection.mutable.Map[(Int,Int), Array[Double]]()
    val cld = Calendar.getInstance()
    val simFormat = new SimpleDateFormat("yyyy-MM-dd")
    dates_arr.map{
      case (date_str, type_id)=>

       cld.setTime( simFormat.parse( date_str ))
       val weekday = cld.get(Calendar.DAY_OF_WEEK)

       (date_str,weekday, type_id)
   }.groupBy( _._2 ).map{
      group =>
        val weekday = group._1
       val feat_map = getFeatMap( group._2 )
        (weekday, feat_map)
    }

  }
  def run( data_pt:String, feat_name:String ): Unit ={
   val date_fp = data_pt + "/dates"
   val dates = IO.load(date_fp).distinct
   val simFormat = new SimpleDateFormat("yyyy-MM-dd")
   dates.foreach{
      date =>
        val feat_dir = data_pt + s"/feature/$feat_name"
        Directory.create( feat_dir)
        val feat_fp = feat_dir + s"/${feat_name}_$date"

        val cld = Calendar.getInstance()
        cld.setTime( simFormat.parse( date ) )
        val weekday = cld.get(Calendar.DAY_OF_WEEK)
        val feats = districts.values.toArray.sorted.flatMap { did =>
           Range(1, 1440 + 1).map {
            ntid =>
              val f1 = stat_map(weekday).getOrElse(ntid,(0,0,0,0,0))
             s"$did,$ntid\t${f1._1},${f1._2},${f1._3},${f1._4},${f1._5}"
          }
        }

        IO.write(feat_fp, feats )
    }
  }

}
