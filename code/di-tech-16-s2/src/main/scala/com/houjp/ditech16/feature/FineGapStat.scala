package com.houjp.ditech16.feature

import java.text.SimpleDateFormat
import java.util.Calendar

import com.houjp.common.Log
import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{OrderAbs, District}
import ditech.common.util.Directory
import org.saddle.Vec

object FineGapStat extends Log{

  def main(args: Array[String]) {
    // 寻找往前 pre 个时间片的gap
    run(ditech16.data_pt,this.getClass.getSimpleName.replace("$","") )

  }

  def run(data_pt: String,feat_name:String): Unit = {
    val districts_fp = data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

    val date_fp = data_pt + "/dates"
    val dates = IO.load(date_fp).distinct

    val pregap_dir = data_pt + s"/feature/${feat_name}"
    Directory.create( pregap_dir )

    dates.foreach { now_date =>
      val pregap_fp = pregap_dir + s"/${feat_name}_$now_date"

      val now_order_abs_fp = data_pt + s"/order_abs_data/order_data_$now_date"
      val now_orders_abs = OrderAbs.load_local(now_order_abs_fp)

      val date_formate = new SimpleDateFormat("yyyy-MM-dd")
      val cal = Calendar.getInstance()
      cal.setTime(date_formate.parse(now_date))
      cal.set(Calendar.DATE, cal.get(Calendar.DATE) - 1)
      val pre_date = date_formate.format(cal.getTime)

      val pre_order_abs_fp = data_pt + s"/order_abs_data/order_data_$pre_date"
      val pre_orders_abs = OrderAbs.load_local(pre_order_abs_fp)

      val pregap_map = Range(1,6).map{
        x =>
          cal_pre_gap(pre_orders_abs, now_orders_abs, (x - 1) * 5)
      }

      val pregap_s = districts.values.toArray.sorted.flatMap { did =>
        Range(1, 1440 + 1).map { ntid =>
          val arr = Range(0,5).map{
            x=>
              pregap_map(x).getOrElse((did, ntid), 0.0)
          }.toArray
          val vec = Vec( arr )
          s"$did,$ntid\t${vec.mean},${vec.median},${vec.stdev},${vec.min.getOrElse(0)},${vec.max.getOrElse(0)}"
        }
      }
      IO.write(pregap_fp, pregap_s)
    }
  }

  def cal_pre_gap(pre_orders: Array[OrderAbs], now_orders: Array[OrderAbs], off: Int): Map[(Int, Int), Double] = {
    val ntid_len = 1440
    val fs = collection.mutable.Map[(Int, Int), Double]()

    pre_orders.foreach { e =>
      val s_ntid = e.new_time_id + 1 + off
      val e_ntid = e.new_time_id + 10 + off

      if (-1 != e.start_district_id &&
        !e.has_driver) {
        Range(ntid_len + 1, e_ntid + 1).foreach { ntid =>
          fs((e.start_district_id, ntid - ntid_len)) = fs.getOrElse((e.start_district_id, ntid - ntid_len), 0.0) + 1.0
        }
      }
    }

    now_orders.foreach { e =>
      val s_ntid = e.new_time_id + 1 + off
      val e_ntid = e.new_time_id + 10 + off

      if (-1 != e.start_district_id &&
        !e.has_driver
      ) {
        Range(s_ntid, math.min(e_ntid, ntid_len) + 1).foreach { ntid =>
          fs((e.start_district_id, ntid - 0)) = fs.getOrElse((e.start_district_id, ntid - 0), 0.0) + 1.0
        }
      }
    }

    fs.toMap
  }

}