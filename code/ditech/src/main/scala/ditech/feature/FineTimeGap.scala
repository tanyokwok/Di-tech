package ditech.feature

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District
import ditech.common.util.Directory
import ditech.datastructure.OrderAbs

object FineTimeGap {

  def main(args: Array[String]) {
    // 寻找往前 pre 个时间片的gap
    run(ditech16.data_pt,this.getClass.getSimpleName.replace("$","") )

  }

  def run(data_pt: String,feat_name:String): Unit = {
    val districts_fp = data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

    val date_fp = data_pt + "/dates"
    val dates = IO.load(date_fp).distinct

    dates.foreach { date =>
      val order_abs_fp = data_pt + s"/order_data/order_data_$date"
      val orders_abs = OrderAbs.load_local(order_abs_fp,districts)

      val pregap_dir = data_pt + s"/fs/${feat_name}"
      Directory.create( pregap_dir )
      val pregap_fp = pregap_dir + s"/${feat_name}_$date"

      val pregap1 = cal_pre_gap(orders_abs, 1)
      val pregap2 = cal_pre_gap(orders_abs, 2)
      val pregap3 = cal_pre_gap(orders_abs, 3)
      val pregap4 = cal_pre_gap(orders_abs, 4)
      val pregap5 = cal_pre_gap(orders_abs, 5)

      val pregap_s = districts.values.toArray.sorted.flatMap { did =>
        Range(1, 145).map { tid =>
          val v1 = pregap1.getOrElse(tid, 0.0)
          val v2 = pregap2.getOrElse(tid, 0.0)
          val v3 = pregap3.getOrElse(tid, 0.0)
          val v4 = pregap4.getOrElse(tid, 0.0)
          val v5 = pregap5.getOrElse(tid, 0.0)
          s"$did,$tid\t$v1,$v2,$v3,$v4,$v5"
        }
      }
      IO.write(pregap_fp, pregap_s)
    }
  }

  def cal_pre_gap(orders: Array[OrderAbs], t_len: Int): Map[Int, Double] = {
    val tid_len = 144
    val fs = collection.mutable.Map[Int, Double]()

    orders.foreach { e =>
      val tid = ( e.fine_time_id + t_len + 2 )/2
      if (-1 != e.start_district_id &&
        !e.has_driver &&
        (tid_len >= tid ) &&
        (1 <= tid )) {
        fs(tid) = fs.getOrElse(tid, 0.0) + 1.0
      }
    }

    fs.toMap
  }

}