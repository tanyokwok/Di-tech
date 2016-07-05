package ditech.feature_bak

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{District, OrderAbs}
import ditech.common.util.Directory

object PreArriveSelf {

  def main(args: Array[String]) {
    // 寻找往前 pre 个时间片的arrive
    run(ditech16.data_pt, this.getClass.getSimpleName.replace("$",""))
  }


  def run(data_pt: String, f_name:String): Unit = {
    val districts_fp = data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

    val date_fp = data_pt + "/dates"
    val dates = IO.load(date_fp).distinct

    dates.foreach { date =>
      val order_abs_fp = data_pt + s"/order_abs_data/order_data_$date"
      val orders_abs = OrderAbs.load_local(order_abs_fp)

      val preArrive_dir= data_pt + s"/fs/${f_name}"
      Directory.create( preArrive_dir )
      val preArrive_fp = preArrive_dir + s"/${f_name}_$date"

      val preArrive1 = cal_pre_Arrive(orders_abs, 1)
      val preArrive2 = cal_pre_Arrive(orders_abs, 2)
      val preArrive3 = cal_pre_Arrive(orders_abs, 3)

      val preArrive_s = districts.values.toArray.sorted.flatMap { did =>
        Range(1, 145).map { tid =>
          val v1 = preArrive1.getOrElse((did, tid), 0.0)
          val v2 = preArrive2.getOrElse((did, tid), 0.0)
          val v3 = preArrive3.getOrElse((did, tid), 0.0)
          s"$did,$tid\t$v1,$v2,$v3"
        }
      }
      IO.write(preArrive_fp, preArrive_s)
    }
  }

  def cal_pre_Arrive(orders: Array[OrderAbs], t_len: Int): Map[(Int, Int), Double] = {
    val tid_len = 144
    val fs = collection.mutable.Map[(Int, Int), Double]()

    orders.foreach { e =>
      if (-1 != e.dest_district_id &&
        e.dest_district_id == e.start_district_id &&
        e.has_driver &&
        (tid_len >= e.time_id + t_len) &&
        (1 <= e.time_id + t_len)) {
        fs((e.dest_district_id, e.time_id + t_len)) = fs.getOrElse((e.dest_district_id, e.time_id + t_len), 0.0) +1.0
      }
    }

    fs.toMap
  }
}