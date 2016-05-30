package com.houjp.ditech16.rule

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{OrderAbs, TimeSlice, District}
import com.houjp.ditech16.feature.PreGap

object PreGapRule {

  def main(args: Array[String]): Unit = {
//    generate_train_ans()
     generate_std_ans()
  }

  def generate_std_ans(): Unit = {
    val ans_fp = ditech16.train_ans_pt + "/std.csv"
    var ans = Array[(Int, String, Int, Double)]()

    val districts_fp = ditech16.s1_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

    val time_slices_fp = ditech16.train_time_slices
    val time_slices = TimeSlice.load_local(time_slices_fp)
    val time_slices_set = time_slices.map { e =>
      (s"${e.year}-${e.month.formatted("%02d")}-${e.day.formatted("%02d")}", e.time_id)
    }.toSet

    time_slices.map { e =>
      s"${e.year}-${e.month.formatted("%02d")}-${e.day.formatted("%02d")}"
    }.distinct.foreach { date =>
      val orders_abs_fp = ditech16.s1_pt + s"/order_abs_data/order_data_$date"
      val orders_abs = OrderAbs.load_local(orders_abs_fp)
      val pregap_1 = PreGap.cal_pre_gap(orders_abs, 0).filter { e =>
        time_slices_set.contains((date, e._1._2))
      }.map { e =>
        (e._1._1, date, e._1._2, e._2)
      }
      ans = ans ++ pregap_1
    }

    IO.write(ans_fp, ans.sorted.map { e =>
      s"${e._1},${e._2}-${e._3},${e._4}"
    })
  }

  def generate_train_ans(): Unit = {
    val ans_fp = ditech16.train_ans_pt + "/ans.csv"
    var ans = collection.mutable.Map[(Int, String), Double]()

    val districts_fp = ditech16.train_cluater_pt + "/cluster_map"
    val districts = District.load_local(districts_fp)

    val time_slices_fp = ditech16.train_time_slices
    val time_slices = TimeSlice.load_local(time_slices_fp)
    val time_slices_set = time_slices.map { e =>
      e.time_slice
    }.toSet

    time_slices.map { e =>
      s"${e.year}-${e.month.formatted("%02d")}-${e.day.formatted("%02d")}"
    }.distinct.foreach { date =>
      val orders_abs_fp = ditech16.train_order_abs_pt + s"/order_data_$date"
      val orders_abs = OrderAbs.load_local(orders_abs_fp)

      // pregap = 1
      PreGap.cal_pre_gap(orders_abs, 1).filter { e =>
        time_slices_set.contains(s"$date-${e._1._2}")
      }.foreach { e =>
        val time_slice = s"$date-${e._1._2}"
        ans((e._1._1, time_slice)) = ans.getOrElse((e._1._1, time_slice), 0.0) + 0.333 * e._2
      }

      // pregap = 2
      PreGap.cal_pre_gap(orders_abs, 2).filter { e =>
        time_slices_set.contains(s"$date-${e._1._2}")
      }.foreach { e =>
        val time_slice = s"$date-${e._1._2}"
        ans((e._1._1, time_slice)) = ans.getOrElse((e._1._1, time_slice), 0.0) + 0.333 * e._2
      }

      // pregap = 3
      PreGap.cal_pre_gap(orders_abs, 3).filter { e =>
        time_slices_set.contains(s"$date-${e._1._2}")
      }.foreach { e =>
        val time_slice = s"$date-${e._1._2}"
        ans((e._1._1, time_slice)) = ans.getOrElse((e._1._1, time_slice), 0.0) + 0.333 * e._2
      }
    }

    IO.write(ans_fp, ans.toArray.map { e =>
      s"${e._1._1},${e._1._2},${e._2}"
    })
  }

}