package ditech.feature_bak

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{District, OrderAbs}
import ditech.common.util.Directory

object Price {

  def main(args: Array[String]) {
    // 寻找往前 pre 个时间片的arrive
    run(ditech16.data_pt,this.getClass.getSimpleName.replace("$","") )
  }

  def run(data_pt: String, f_name:String): Unit = {
    val districts_fp = data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

    val date_fp = data_pt + "/dates"
    val dates = IO.load(date_fp).distinct

    dates.foreach { date =>
      val order_abs_fp = data_pt + s"/order_abs_data/order_data_$date"
      val orders_abs = OrderAbs.load_local(order_abs_fp)

      val price_dir= data_pt + s"/fs/${f_name}"
      Directory.create( price_dir )
      val price_fp = price_dir + s"/${f_name}_$date"

      val price1 = cal_price(orders_abs, 1)
      val price2 = cal_price(orders_abs, 2)
      val price3 = cal_price(orders_abs, 3)

      val price_s = districts.values.toArray.sorted.flatMap { did =>
        Range(1, 145).map { tid =>
          val feat = new StringBuilder(s"$did,$tid\t")
          Range(0,5).foreach{
            pid =>
              val v1 = price1.getOrElse((did, tid,pid), 0.0)
              val v2 = price2.getOrElse((did, tid,pid), 0.0)
              val v3 = price3.getOrElse((did, tid,pid), 0.0)
              feat.append(s"$v1,$v2,$v3,")
          }
          feat.substring(0, feat.length - 1)
        }
      }
      IO.write(price_fp, price_s)
    }
  }

  def cal_price(orders: Array[OrderAbs], t_len: Int): Map[(Int,Int, Int), Double] = {
    val tid_len = 144
    val fs = collection.mutable.Map[(Int,Int, Int), Double]()

    orders.foreach { e =>
      if (-1 != e.dest_district_id &&
        e.has_driver &&
        (tid_len >= e.time_id + t_len) &&
        (1 <= e.time_id + t_len)) {
        if( e.price <= 5 ){
          fs((e.dest_district_id, e.time_id + t_len,0)) = fs.getOrElse((e.dest_district_id, e.time_id + t_len,0), 0.0) +1.0
        }else if( e.price <= 10 ){
          fs((e.dest_district_id, e.time_id + t_len,1)) = fs.getOrElse((e.dest_district_id, e.time_id + t_len,1), 0.0) +1.0
        }else if( e.price <= 20 ){
          fs((e.dest_district_id, e.time_id + t_len,2)) = fs.getOrElse((e.dest_district_id, e.time_id + t_len,2), 0.0) +1.0
        }else if( e.price <= 30 ){
          fs((e.dest_district_id, e.time_id + t_len,3)) = fs.getOrElse((e.dest_district_id, e.time_id + t_len,3), 0.0) +1.0
        }else
          fs((e.dest_district_id, e.time_id + t_len,4)) = fs.getOrElse((e.dest_district_id, e.time_id + t_len,4), 0.0) +1.0
      }
    }

    fs.toMap
  }
}