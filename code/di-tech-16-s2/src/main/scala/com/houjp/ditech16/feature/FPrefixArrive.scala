package com.houjp.ditech16.feature

import java.text.SimpleDateFormat
import java.util.Calendar

import com.houjp.common.Log
import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{OrderAbs, District}
import ditech.common.util.Directory

object FPrefixArrive extends Log{
 val districts_fp = ditech16.data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

  def main(args: Array[String]) {
    // 寻找往前 pre 个时间片的gap
    run(ditech16.data_pt,this.getClass.getSimpleName.replace("$",""))
  }

  def run(data_pt: String,f_name:String): Unit = {

    val date_fp = data_pt + "/dates"
    val dates = IO.load(date_fp).distinct

    val pregap_dir = data_pt + s"/feature/${f_name}"
    Directory.create( pregap_dir )
    dates.foreach { now_date =>
      val now_order_abs_fp = data_pt + s"/order_abs_data/order_data_$now_date"
      val now_orders_abs = OrderAbs.load_local(now_order_abs_fp)

      val date_formate = new SimpleDateFormat("yyyy-MM-dd")
      val cal = Calendar.getInstance()
      cal.setTime(date_formate.parse(now_date))
      cal.set(Calendar.DATE, cal.get(Calendar.DATE) - 1)
      val pre_date = date_formate.format(cal.getTime)

      val pre_orders_abs = OrderAbs.load_local(data_pt + s"/order_abs_data/order_data_$pre_date")

      log(s"pre_date($pre_date),now_date($now_date)")

      val now_min_fs = cal_minite_gap(now_orders_abs)
      val pre_min_fs = cal_minite_gap(pre_orders_abs)

      val pregap_fp = pregap_dir + s"/${f_name}_$now_date"

      val arrive_map = Range(1,16).map{
        pre =>
          (pre, cal_pre_gap(pre_min_fs, now_min_fs, pre*2))
      }.toMap

      val pregap_s = districts.values.toArray.sorted.flatMap { did =>
        Range(1, 1440 + 1).map { ntid =>
          val feats = new StringBuilder(s"$did,$ntid\t")
          Range(1,16).foreach{
            pre =>
              val v = arrive_map(pre).getOrElse((did, ntid), 0.0)
              feats.append( s"$v,")
          }
          feats.substring(0,feats.length - 1 )
        }
      }
      IO.write(pregap_fp, pregap_s)
    }
  }

  def cal_minite_gap( orders: Array[OrderAbs]): Map[(Int, Int), Double] ={
    val fs = collection.mutable.Map[(Int, Int), Double]()
    orders.foreach{
      e=>
       if (-1 != e.dest_district_id && e.has_driver){
        fs((e.dest_district_id, e.new_time_id)) =
          fs.getOrElse((e.dest_district_id, e.new_time_id), 0.0) + 1.0
      }
    }
    fs.toMap
  }
  def cal_pre_gap(pre_min_fs:Map[(Int,Int),Double], now_min_fs:Map[(Int,Int),Double], t_len: Int): Map[(Int, Int), Double] = {
    val new_tid_len = 1440
    val fs = collection.mutable.Map[(Int, Int), Double]()

    districts.values.toArray.distinct.sorted.foreach{
      did =>
        Range(1,1440 + 1).foreach{
          ntid =>
            Range(0,t_len).foreach{
              x =>
                val min_id = (ntid - 1 )- x
                if (min_id < 1) {
                  fs((did, ntid)) = fs.getOrElse((did, ntid), 0.0) + pre_min_fs.getOrElse((did, min_id + 1440), 0.0)
                } else {
                  fs((did, ntid)) = fs.getOrElse((did, ntid), 0.0) + now_min_fs.getOrElse((did, min_id + 0), 0.0)
                }
            }
        }
    }
    fs.toMap
  }

}