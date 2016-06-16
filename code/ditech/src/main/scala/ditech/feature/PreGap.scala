package ditech.feature

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{District, Order, OrderAbs, TimeSlice}
import ditech.common.util.Directory

object PreGap {

  def main(args: Array[String]) {
    // 寻找往前 pre 个时间片的gap
    run(ditech16.s1_pt, this.getClass.getSimpleName.replace("$",""))
  }

  def run(data_pt: String, f_name:String): Unit = {
    val districts_fp = data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

    val date_fp = data_pt + "/dates"
    val dates = IO.load(date_fp).distinct

    dates.foreach { date =>
      val order_abs_fp = data_pt + s"/order_abs_data/order_data_$date"
      val orders_abs = OrderAbs.load_local(order_abs_fp)

      val pregap_dir = data_pt + s"/fs/${f_name}"
      Directory.create( pregap_dir )
      val pregap_fp = pregap_dir + s"/${f_name}_$date"

      val pregap1 = cal_pre_gap(orders_abs, 1)
      val pregap2 = cal_pre_gap(orders_abs, 2)
      val pregap3 = cal_pre_gap(orders_abs, 3)

      val pregap_s = districts.values.toArray.sorted.flatMap { did =>
        Range(1, 145).map { tid =>
          val v1 = pregap1.getOrElse((did, tid), 0.0)
          val v2 = pregap2.getOrElse((did, tid), 0.0)
          val v3 = pregap3.getOrElse((did, tid), 0.0)
          s"$did,$tid\t$v1,$v2,$v3"
        }
      }
      IO.write(pregap_fp, pregap_s)
    }
  }

  def cal_pre_gap(orders: Array[OrderAbs], t_len: Int): Map[(Int, Int), Double] = {
    val tid_len = 144
    val fs = collection.mutable.Map[(Int, Int), Double]()

    orders.foreach { e =>
      if (-1 != e.start_district_id &&
        !e.has_driver &&
        (tid_len >= e.time_id + t_len) &&
        (1 <= e.time_id + t_len)) {
        fs((e.start_district_id, e.time_id + t_len)) = fs.getOrElse((e.start_district_id, e.time_id + t_len), 0.0) + 1.0
      }
    }

    fs.toMap
  }

  def run_local(orders: Array[Order], time_slices: Array[TimeSlice], t_len: Int): Array[(Int, TimeSlice, Double)] = {

    time_slices.flatMap { time_slice =>
      val cnt = collection.mutable.Map[Int, Int]()
      orders.filter(e => e.year == time_slice.year &&
        e.month == time_slice.month &&
        e.day == time_slice.day &&
        e.time_id == time_slice.time_id - t_len &&
        e.driver_id == "NULL" &&
        e.start_district_id != -1
      ).foreach { order =>
        cnt(order.start_district_id) = cnt.getOrElse(order.start_district_id, 0) + 1
      }
      cnt.toArray.map {
        case (did: Int, gap: Int) =>
          (did, time_slice, gap.toDouble)
      }
    }
  }
}