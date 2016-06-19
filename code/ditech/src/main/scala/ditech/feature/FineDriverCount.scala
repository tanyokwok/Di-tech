package ditech.feature

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District
import ditech.common.util.Directory
import ditech.datastructure.Order

object FineDriverCount {

  def main(args: Array[String]) {
    // 寻找往前 pre 个时间片的gap
    run(ditech16.s1_pt,this.getClass.getSimpleName.replace("$","") )
  }

  def run(data_pt: String,feat_name:String): Unit = {
    val districts_fp = data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

    val date_fp = data_pt + "/dates"
    val dates = IO.load(date_fp).distinct

    dates.foreach { date =>
      val order_fp = data_pt + s"/order_data/order_data_$date"
      val orders = Order.load_local(order_fp,districts)

      val pregap_dir = data_pt + s"/fs/${feat_name}"
      Directory.create( pregap_dir )
      val pregap_fp = pregap_dir + s"/${feat_name}_$date"

      val pregap1 = cal_pre_gap(orders, 1)
      val pregap2 = cal_pre_gap(orders, 2)
      val pregap3 = cal_pre_gap(orders, 3)
      val pregap4 = cal_pre_gap(orders, 4)
      val pregap5 = cal_pre_gap(orders, 5)

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

  def cal_pre_gap(orders: Array[Order], t_len: Int): Map[Int, Int] = {
    orders.map{ e =>
      val tid = ( e.fine_id + t_len + 2 )/2
       (tid,e.driver_id)
    }.groupBy( _._1 ).map{
      group =>
        val tid = group._1
        val size = group._2.filter{
          case (tid,did)=>
            !"NULL".equals( did )
        }.distinct.size
        (tid, size)
    }.toMap
  }

}