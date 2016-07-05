package ditech.preprocess

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{District, OrderAbs, TimeSlice}
import ditech.feature.PreGap

object Result {

  def main(args: Array[String]): Unit = {
    generate_std_ans(ditech16.ans_pt + "/test_std.csv", ditech16.train_pt + "/test_time_slices")
    generate_std_ans(ditech16.ans_pt + "/val_std1.csv", ditech16.train_pt + "/val_time_slices1")
    generate_std_ans(ditech16.ans_pt + "/val_std2.csv", ditech16.train_pt + "/val_time_slices2")
    generate_std_ans(ditech16.ans_pt + "/val_std.csv", ditech16.test1_pt + "/val_time_slices")
  }

  def generate_std_ans(ans_fp: String, time_slices_fp: String): Unit = {
    //    var ans = Array[(Int, String, Int, Double)]()

    val districts_fp = ditech16.data_pt + "/cluster_map/cluster_map"
    val districts = District.loadDidTypeId(districts_fp).values.toArray.sortBy(_._1)

    val time_slices = TimeSlice.load_local(time_slices_fp)
    val time_slices_set = time_slices.map { e =>
      (s"${e.year}-${e.month.formatted("%02d")}-${e.day.formatted("%02d")}", e.time_id)
    }.toSet

    val results = time_slices.map { e =>
      (s"${e.year}-${e.month.formatted("%02d")}-${e.day.formatted("%02d")}", e.time_id)
    }.groupBy(_._1).flatMap { group =>

      val date = group._1
      val orders_abs_fp = ditech16.data_pt + s"/order_abs_data/order_data_$date"
      val orders_abs = OrderAbs.load_local(orders_abs_fp)
      val pregap_1 = PreGap.cal_pre_gap(orders_abs, 0)

      districts.distinct.filter{
        case (did,tp)=>
          tp == 2 || tp == 0
      }.flatMap {
        case (did,tp) =>
          group._2.map {
            case (date, tid) =>
              val v = pregap_1.getOrElse((did, tid), 0.0)
              s"$did,${date}-${tid},$v"
          }

      }
    }.toArray

    IO.write(ans_fp, results)
  }


}