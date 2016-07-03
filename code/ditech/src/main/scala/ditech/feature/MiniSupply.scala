package ditech.feature

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District
import ditech.common.util.Directory
import ditech.datastructure.OrderAbs

object MiniSupply {

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
      val arrive_map = Range(0,21).map{
        pre =>
          (pre, cal_pre_gap(orders_abs, pre))
      }.toMap

      val pregap_s = districts.values.toArray.sorted.flatMap { did =>
        Range(1, 145).map { tid =>
          val feats = new StringBuilder(s"$did,$tid\t")
          Range(0,21).foreach{
            pre =>
              val v = arrive_map(pre).getOrElse((did, tid), 0.0)
              feats.append( s"$v,")
          }
          feats.substring(0,feats.length - 1 )
        }
      }
      IO.write(pregap_fp, pregap_s)
    }
  }

  def cal_pre_gap(orders: Array[OrderAbs], t_len: Int): Map[(Int, Int), Double] = {
    val tid_len = 144
    val fs = collection.mutable.Map[(Int, Int), Double]()

    orders.foreach { e =>
      val tid = ( e.min_time_id + 9 + t_len )/10 + 1
      if (-1 != e.start_district_id && e.has_driver ) {
        fs((e.start_district_id, tid )) = fs.getOrElse((e.start_district_id, tid ), 0.0) + 1.0
      }
    }

    fs.toMap
  }

}