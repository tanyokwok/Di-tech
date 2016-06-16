package ditech.feature

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{District, OrderAbs}
import ditech.common.util.Directory

object FDemand {

  def main(args: Array[String]) {
    // 寻找往前 pre 个时间片的arrive
    run(ditech16.s1_pt, FDemand.getClass.getSimpleName.replace("$",""))
  }

  def run(data_pt: String, feat_name:String): Unit = {
    val districts_fp = data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

    val date_fp = data_pt + "/dates"
    val dates = IO.load(date_fp).distinct

    dates.foreach { date =>
      val order_abs_fp = data_pt + s"/order_abs_data/order_data_$date"
      val orders_abs = OrderAbs.load_local(order_abs_fp)

      val demand_dir= data_pt + s"/fs/$feat_name"
      Directory.create( demand_dir )
      val demand_fp = demand_dir + s"/${feat_name}_$date"

      val demand1 = cal_demand(orders_abs, 1)
      val demand2 = cal_demand(orders_abs, 2)
      val demand3 = cal_demand(orders_abs, 3)

      val demand_s = districts.values.toArray.sorted.flatMap { did =>
        Range(1, 145).map { tid =>
          val v1 = demand1.getOrElse((did, tid), 0.0)
          val v2 = demand2.getOrElse((did, tid), 0.0)
          val v3 = demand3.getOrElse((did, tid), 0.0)
          s"$did,$tid\t$v1,$v2,$v3"
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