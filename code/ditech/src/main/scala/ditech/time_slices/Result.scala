package ditech.time_slices

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{District, OrderAbs, TimeSlice}
import com.houjp.ditech16.feature.PreGap

object Result {

  def main(args: Array[String]): Unit = {
    generate_std_ans(ditech16.train_ans_pt + "/test_std.csv",ditech16.train_pt +"/test_time_slices")
    generate_std_ans(ditech16.train_ans_pt + "/val_std1.csv",ditech16.train_pt +"/val_time_slices1")
    generate_std_ans(ditech16.train_ans_pt + "/val_std2.csv",ditech16.train_pt +"/val_time_slices2")
  }

  def generate_std_ans(ans_fp:String,time_slices_fp:String): Unit = {
    var ans = Array[(Int, String, Int, Double)]()

    val districts_fp = ditech16.s1_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

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

}