package ditech.feature

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District
import ditech.common.util.Directory
import ditech.datastructure.OrderAbs
import org.saddle.Vec

object FineGapStat {

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

      val pregap_map = Range(1,6).map{
        x =>
          cal_pre_gap(orders_abs, x)
      }

      val pregap_s = districts.values.toArray.sorted.flatMap { did =>
        Range(1, 145).map { tid =>
          val arr = Range(0,5).map{
            x=>
              pregap_map(x).getOrElse((did, tid), 0.0)
          }.toArray
          val vec = Vec( arr )
          s"$did,$tid\t${vec.mean},${vec.median},${vec.stdev},${vec.min.getOrElse(0)},${vec.max.getOrElse(0)}"
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
      if (-1 != e.start_district_id &&
        !e.has_driver &&
        (tid_len >= tid ) &&
        (1 <= tid )) {
        fs((e.start_district_id, tid )) = fs.getOrElse((e.start_district_id, tid ), 0.0) + 1.0
      }
    }

    fs.toMap
  }

}