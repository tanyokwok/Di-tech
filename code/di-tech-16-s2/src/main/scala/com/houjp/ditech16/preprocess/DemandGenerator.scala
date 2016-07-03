package com.houjp.ditech16.preprocess

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{District, OrderAbs}

object DemandGenerator {

  def main(args: Array[String]) {
    run(ditech16.data_pt)
  }

  def run(data_pt: String): Unit = {
    val districts_fp = data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

    val dates_fp = data_pt + "/dates"
    val dates = IO.load(dates_fp).distinct

    dates.foreach { date =>
      val order_abs_fp = data_pt + s"/order_abs_data/order_data_$date"
      val label_fp = data_pt + s"/label/label_$date"

      generate_label(order_abs_fp, districts, label_fp)
    }

  }

  def generate_label(order_abs_fp: String, districts: Map[String, Int], label_fp: String) = {
    val tid_len = 144

    val did_tid_gap = collection.mutable.Map[(Int, Int), Double]()
    OrderAbs.load_local(order_abs_fp).foreach { e =>
      if (!e.has_driver && (-1 != e.start_district_id)) {
        did_tid_gap((e.start_district_id, e.time_id)) = did_tid_gap.getOrElse((e.start_district_id, e.time_id), 0.0) + 1.0
      }
    }
    val labels = districts.values.toArray.sorted.flatMap { did =>
      Range(1, tid_len + 1).map { tid =>
//        val gap = math.max( 1.0, did_tid_gap.getOrElse((did, tid), 0.0) )
        val gap = did_tid_gap.getOrElse( (did,tid), 0.0 )
        s"$did,$tid\t$gap"
      }
    }
    IO.write(label_fp, labels)
  }
}
