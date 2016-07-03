package com.houjp.ditech16.preprocess

import java.text.SimpleDateFormat
import java.util.{Date, Calendar}

import com.houjp.common.Log
import com.houjp.common.io.IO
import com.houjp.ditech16.datastructure.{District, OrderAbs}
import com.houjp.ditech16

object LabelGenerator extends Log {

  def main(args: Array[String]) {
    run(ditech16.data_pt)
  }

  def run(data_pt: String): Unit = {
    val districts_fp = data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

    val dates_fp = data_pt + "/dates"
    val dates = IO.load(dates_fp).distinct

    dates.foreach { now_date =>
      val date_formate = new SimpleDateFormat("yyyy-MM-dd")
      val cal = Calendar.getInstance()
      cal.setTime(date_formate.parse(now_date))
      cal.set(Calendar.DATE, cal.get(Calendar.DATE) + 1)
      val aft_date = date_formate.format(cal.getTime)

      log(s"aft_date($aft_date),now_date($now_date)")


      val aft_order_abs_fp = data_pt + s"/order_abs_data/order_data_$aft_date"
      val now_order_abs_fp = data_pt + s"/order_abs_data/order_data_$now_date"
      val label_fp = data_pt + s"/label/label_$now_date"

      generate_label(aft_order_abs_fp, now_order_abs_fp, districts, label_fp)
    }

  }

  def generate_label(aft_order_abs_fp: String, now_order_abs_fp: String, districts: Map[String, Int], label_fp: String) = {
    val new_tid_len = 1440

    val did_ntid_gap = collection.mutable.Map[(Int, Int), Double]()

    OrderAbs.load_local(aft_order_abs_fp).foreach { e =>
      if (!e.has_driver && (-1 != e.start_district_id) && (e.new_time_id < 10)) {
        Range(1440 + e.new_time_id - 9, 1440 + 1).foreach { t_ntid =>
          did_ntid_gap((e.start_district_id, t_ntid)) = did_ntid_gap.getOrElse((e.start_district_id, t_ntid), 0.0) + 1.0
        }
      }
    }

    OrderAbs.load_local(now_order_abs_fp).foreach { e =>
      if (!e.has_driver && (-1 != e.start_district_id)) {
        val s_ntid = math.max(1, e.new_time_id - 9)
        Range(s_ntid, e.new_time_id + 1).foreach { t_ntid =>
          did_ntid_gap((e.start_district_id, t_ntid)) = did_ntid_gap.getOrElse((e.start_district_id, t_ntid), 0.0) + 1.0
        }
      }
    }

    val labels = districts.values.toArray.sorted.flatMap { did =>
      Range(1, new_tid_len + 1).map { ntid =>
        val gap = did_ntid_gap.getOrElse( (did,ntid), 0.0 )
        s"$did,$ntid\t$gap"
      }
    }
    IO.write(label_fp, labels)
  }
}
