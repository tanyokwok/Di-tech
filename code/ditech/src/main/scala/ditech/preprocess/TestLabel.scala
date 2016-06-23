package ditech.preprocess

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{District, OrderAbs, TimeSlice}
import ditech.feature.PreGap

object TestLabel {

  def main(args: Array[String]): Unit = {
    generate_std_ans(ditech16.data_pt + "/test_label.csv",ditech16.train_pt +"/test_time_slices")
  }

  def generate_std_ans(ans_fp:String,time_slices_fp:String): Unit = {
    var ans = Array[(Int, String, Int, Double)]()

    val districts_fp = ditech16.data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

    val time_slices = TimeSlice.load_local(time_slices_fp)
    val time_slices_set = time_slices.map { e =>
      (s"${e.year}-${e.month.formatted("%02d")}-${e.day.formatted("%02d")}", e.time_id)
    }.toSet

    time_slices.map { e =>
      s"${e.year}-${e.month.formatted("%02d")}-${e.day.formatted("%02d")}"
    }.distinct.foreach { date =>
      val orders_abs_fp = ditech16.data_pt + s"/order_abs_data/order_data_$date"
      val orders_abs = OrderAbs.load_local(orders_abs_fp)
       val pregap_1: Map[(Int, Int), Double] = PreGap.cal_pre_gap(orders_abs, 0)

     val gap =  time_slices_set.filter{
       time =>
         time._1.equals( date)
     }.flatMap{
       case (date, tid) =>
          Range(1,67).map{
            did=>
              (did, date, tid, pregap_1.getOrElse((did,tid),0.0))
          }
      }.toArray

      ans = ans ++ gap
    }

    IO.write(ans_fp, ans.sorted.map { e =>
      s"${e._1},${e._2}-${e._3},${e._4}"
    })
  }




}