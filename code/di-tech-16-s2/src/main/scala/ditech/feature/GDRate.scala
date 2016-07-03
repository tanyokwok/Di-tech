package ditech.feature

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District
import ditech.common.util.Directory
import ditech.datastructure.OrderAbs

object GDRate {

  def main(args: Array[String]) {
    // 寻找往前 pre 个时间片的gap
    run(ditech16.data_pt,this.getClass.getSimpleName.replace("$",""))
  }

  def run(data_pt: String,f_name:String): Unit = {
    val districts_fp = data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

    val date_fp = data_pt + "/dates"
    val dates = IO.load(date_fp).distinct

      val pregap_dir = data_pt + s"/fs/${f_name}"
      Directory.create( pregap_dir )
    dates.foreach { date =>
      val order_abs_fp = data_pt + s"/order_data/order_data_$date"
      val orders_abs = OrderAbs.load_local(order_abs_fp,districts)

      val pregap_fp = pregap_dir + s"/${f_name}_$date"

      val feat_map = Range(1,4).map{
        pre =>
          val (pregap,demand) = cal_pre_gap(orders_abs, pre)
          (pre, (pregap,demand))
      }.toMap

      val pregap_s = districts.values.toArray.sorted.flatMap { did =>
        Range(1, 145).map { tid =>
          val builder = new StringBuilder(s"$did,$tid\t")
          Range(1,4).foreach {
            pre =>
              val (gap, demand) = feat_map(pre)
              val v = gap.getOrElse((did, tid), 0.0)
              val d = demand.getOrElse((did, tid), 0.0)
              builder.append( s"${v/(d+1e-6)},")
          }

          builder.substring(0, builder.length - 1 )
        }
      }
      IO.write(pregap_fp, pregap_s)
    }
  }

  def cal_pre_gap(orders: Array[OrderAbs], t_len: Int): (Map[(Int, Int), Double],Map[(Int, Int), Double])  = {
    val tid_len = 144
    val gap = collection.mutable.Map[(Int, Int), Double]()
    val demand = collection.mutable.Map[(Int, Int), Double]()

    orders.foreach { e =>
      val tid = e.time_id + t_len
      if (-1 != e.start_district_id &&
        !e.has_driver &&
        (tid_len >= tid ) &&
        (1 <= tid )) {
        gap((e.start_district_id, tid )) = gap.getOrElse((e.start_district_id, tid ), 0.0) + 1.0
      }
       if (-1 != e.start_district_id &&
        (tid_len >= tid ) &&
        (1 <= tid )) {
        demand((e.start_district_id, tid )) = demand.getOrElse((e.start_district_id, tid ), 0.0) + 1.0
      }
    }

    (gap.toMap, demand.toMap)
  }

}