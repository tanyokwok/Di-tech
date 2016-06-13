package ditech.feature

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District
import ditech.common.util.Directory
import ditech.datastructure.OrderAbs

object FineArrive {

  def main(args: Array[String]) {
    // 寻找往前 pre 个时间片的gap
    run(ditech16.s1_pt, "fineArrive")
  }

  def run(data_pt: String,feat_name:String): Unit = {
    val districts_fp = data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

    val pregap_dir = data_pt + s"/fs/${feat_name}"
    Directory.create( pregap_dir )
    val date_fp = data_pt + "/dates"
    val dates = IO.load(date_fp).distinct

    dates.foreach { date =>
      val pregap_fp = pregap_dir + s"/${feat_name}_$date"
      val order_abs_fp = data_pt + s"/order_data/order_data_$date"
      val orders_abs = OrderAbs.load_local(order_abs_fp,districts)

      val arrive1 = cal_pre_gap(orders_abs, 1)
      val arrive2 = cal_pre_gap(orders_abs, 2)
      val arrive3 = cal_pre_gap(orders_abs, 3)
      val arrive4 = cal_pre_gap(orders_abs, 4)
      val arrive5 = cal_pre_gap(orders_abs, 5)

      val pregap_s = districts.values.toArray.sorted.flatMap { did =>
        Range(1, 145).map { tid =>
          val v1 = arrive1.getOrElse((did, tid), 0.0)
          val v2 = arrive2.getOrElse((did, tid), 0.0)
          val v3 = arrive3.getOrElse((did, tid), 0.0)
          val v4 = arrive4.getOrElse((did, tid), 0.0)
          val v5 = arrive5.getOrElse((did, tid), 0.0)
          s"$did,$tid\t$v1,$v2,$v3,$v4,$v5"
        }
      }
      IO.write(pregap_fp, pregap_s)
    }
  }

  def cal_pre_gap(orders: Array[OrderAbs], t_len: Int): Map[(Int, Int), Double] = {
    val tid_len = 144
    val fs = collection.mutable.Map[(Int, Int), Double]()

    orders.foreach { e =>
      val tid = ( e.fine_time_id + t_len + 2 )/2
      if (-1 != e.dest_district_id &&
        e.has_driver &&
        (tid_len >= tid ) &&
        (1 <= tid )) {
        fs((e.dest_district_id, tid )) = fs.getOrElse((e.dest_district_id, tid ), 0.0) + 1.0
      }
    }

    fs.toMap
  }

}