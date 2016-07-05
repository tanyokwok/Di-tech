package com.houjp.ditech16.feature

import java.text.SimpleDateFormat
import java.util.Calendar

import com.houjp.common.Log
import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{OrderAbs, District}
import ditech.common.util.Directory

object FineDemand extends Log{

  def main(args: Array[String]) {
    // 寻找往前 pre 个时间片的gap
    run(ditech16.data_pt,FineDemand.getClass.getSimpleName.replace("$",""))
  }

  def run(data_pt: String, feat_name:String): Unit = {
    val districts_fp = data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

    val pregap_dir = data_pt + s"/feature/$feat_name"
    Directory.create( pregap_dir )
    val date_fp = data_pt + "/dates"
    val dates = IO.load(date_fp).distinct

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

      log(s"pre_date($pre_date),now_date($now_date)")

      val arrive1 = cal_pre_gap(pre_orders_abs, now_orders_abs, 0)
      val arrive2 = cal_pre_gap(pre_orders_abs, now_orders_abs, 5)
      val arrive3 = cal_pre_gap(pre_orders_abs, now_orders_abs, 10)
      val arrive4 = cal_pre_gap(pre_orders_abs, now_orders_abs, 15)
      val arrive5 = cal_pre_gap(pre_orders_abs, now_orders_abs, 20)

      val pregap_s = districts.values.toArray.sorted.flatMap { did =>
        Range(1, 1440 + 1).map { ntid =>
          val v1 = arrive1.getOrElse((did, ntid), 0.0)
          val v2 = arrive2.getOrElse((did, ntid), 0.0)
          val v3 = arrive3.getOrElse((did, ntid), 0.0)
          val v4 = arrive4.getOrElse((did, ntid), 0.0)
          val v5 = arrive5.getOrElse((did, ntid), 0.0)
          s"$did,$ntid\t$v1,$v2,$v3,$v4,$v5"
        }
      }
      IO.write(pregap_fp, pregap_s)
    }
  }

  def cal_pre_gap(pre_orders: Array[OrderAbs], now_orders: Array[OrderAbs], off: Int): Map[(Int, Int), Double] = {
    val tid_len = 1440
    val fs = collection.mutable.Map[(Int, Int), Double]()

    pre_orders.foreach { e =>
      val s_ntid = e.new_time_id + 1 + off
      val e_ntid = e.new_time_id + 10 + off

      if (-1 != e.start_district_id) {
        Range(tid_len + 1, e_ntid + 1).foreach { ntid =>
          fs((e.start_district_id, ntid - tid_len)) = fs.getOrElse((e.start_district_id, ntid - tid_len), 0.0) + 1.0
        }
      }
    }

    now_orders.foreach { e =>
      val s_ntid = e.new_time_id + 1 + off
      val e_ntid = e.new_time_id + 10 + off

      if (-1 != e.start_district_id) {
        Range(s_ntid, math.min(e_ntid, tid_len) + 1).foreach { ntid =>
          fs((e.start_district_id, ntid - 0)) = fs.getOrElse((e.start_district_id, ntid - 0), 0.0) + 1.0
        }
      }
    }

    fs.toMap
  }

}