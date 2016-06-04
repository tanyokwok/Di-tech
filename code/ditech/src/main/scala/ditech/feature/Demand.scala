package ditech.feature

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{District, OrderAbs}
import ditech.common.util.Directory

object Demand {

  def main(args: Array[String]) {
    // 寻找往前 pre 个时间片的arrive
    run(ditech16.s1_pt, 1)
    run(ditech16.s1_pt, 2)
    run(ditech16.s1_pt, 3)
  }

  def run(data_pt: String, pre: Int): Unit = {
    val districts_fp = data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

    val date_fp = data_pt + "/dates"
    val dates = IO.load(date_fp).distinct

    dates.foreach { date =>
      val order_abs_fp = data_pt + s"/order_abs_data/order_data_$date"
      val orders_abs = OrderAbs.load_local(order_abs_fp)

      val demand_dir= data_pt + s"/fs/demand_$pre"
      Directory.create( demand_dir )
      val demand_fp = demand_dir + s"/demand_${pre}_$date"

      val demand = cal_demand(orders_abs, pre)

      val demand_s = districts.values.toArray.sorted.flatMap { did =>
        Range(1, 145).map { tid =>
          val v = demand.getOrElse((did, tid), 0.0)
          s"$did,$tid\t$v"
        }
      }
      IO.write(demand_fp, demand_s)
    }
  }

  def cal_demand(orders: Array[OrderAbs], t_len: Int): Map[(Int, Int), Double] = {
    val tid_len = 144
    val fs = collection.mutable.Map[(Int, Int), Double]()

    orders.foreach { e =>
      if (-1 != e.start_district_id &&
        (tid_len >= e.time_id + t_len) &&
        (1 <= e.time_id + t_len)) {
        fs((e.start_district_id, e.time_id + t_len)) = fs.getOrElse((e.start_district_id, e.time_id + t_len), 0.0) +1.0
      }
    }

    fs.toMap
  }
}