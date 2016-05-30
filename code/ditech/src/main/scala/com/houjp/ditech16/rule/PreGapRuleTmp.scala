package com.houjp.ditech16.rule

import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{Ans, District, Order, TimeSlice}
import com.houjp.ditech16.feature.PreGap
import com.houjp.common.io.IO

object PreGapRuleTmp {

  def main(args: Array[String]) {
    val districts_fp = ditech16.test1_cluater_pt + "/cluster_map"
    val districts = District.load_local(districts_fp)

    val time_slices_fp = ditech16.test1_time_slices
    val time_slices = TimeSlice.load_local(time_slices_fp)

    val orders_fp = ditech16.test1_order_pt + "/order_data_test"
    val orders = Order.load_local(orders_fp, districts)

    val pre_gap_1_fp = ditech16.ans_pt + "/20160522_pregap1.csv"
    val pre_gap_2_fp = ditech16.ans_pt + "/20160522_pregap2.csv"
    val pre_gap_3_fp = ditech16.ans_pt + "/20160522_pregap3.csv"

    val pre_gap_1 = PreGap.run_local(orders, time_slices, 1)
    val pre_gap_2 = PreGap.run_local(orders, time_slices, 2)
    val pre_gap_3 = PreGap.run_local(orders, time_slices, 3)

    // IO.write(pre_gap_2_fp, pre_gap_2.map(e => Ans.toAns(e).toString))

    // calculate avg
    val ks = collection.mutable.Map() ++ (pre_gap_1.map(e => ((e._1, e._2.time_slice), 0.0)) ++
      pre_gap_2.map(e => ((e._1, e._2.time_slice), 0.0)) ++
      pre_gap_3.map(e => ((e._1, e._2.time_slice), 0.0))).distinct.toMap
    pre_gap_1.foreach { e =>
      ks((e._1, e._2.time_slice)) += 0.334 * e._3
    }
    pre_gap_2.foreach { e =>
      ks((e._1, e._2.time_slice)) += 0.333 * e._3
    }
    pre_gap_3.foreach { e =>
      ks((e._1, e._2.time_slice)) += 0.333 * e._3
    }
    val pre_gap_ave_123 = ditech16.ans_pt + "20160525_pregapave123.csv"

    IO.write(pre_gap_ave_123, ks.toArray.map { e =>
      s"${e._1._1},${e._1._2},${e._2}"
    })
  }
}