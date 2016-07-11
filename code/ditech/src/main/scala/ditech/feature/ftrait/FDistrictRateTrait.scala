package ditech.feature.ftrait

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District
import ditech.common.util.Directory
import ditech.datastructure.OrderAbs

trait FDistrictRateTrait {
  val districts_fp = ditech16.data_pt + "/cluster_map/cluster_map"
  val districts = District.load_local(districts_fp)

  val date_fp = ditech16.data_pt + "/dates"
  val dates = IO.load(date_fp).distinct

  def run(data_pt: String,feat_name:String): Unit = {

    dates.foreach { date =>
      val order_abs_fp = data_pt + s"/order_data/order_data_$date"
      val orders_abs = OrderAbs.load_local(order_abs_fp,districts)

      val pregap_dir = data_pt + s"/fs/${feat_name}"
      Directory.create( pregap_dir )
      val pregap_fp = pregap_dir + s"/${feat_name}_$date"

      val district_gaps = cal_pre_gap(orders_abs, 1)
//      val pregap2 = cal_pre_gap(orders_abs, 2)
//      val pregap3 = cal_pre_gap(orders_abs, 3)
//      val pregap4 = cal_pre_gap(orders_abs, 4)
//      val pregap5 = cal_pre_gap(orders_abs, 5)

      val feat_lines = districts.values.toArray.sorted.flatMap { did =>
        Range(1, ditech16.max_time_id + 1).map { tid =>
          val arr: Array[Double] = district_gaps( tid )
          val feat_s = new StringBuilder(s"$did,$tid\t")
          arr.foreach{
            a =>
              feat_s.append(s"$a,")
          }
          feat_s.substring(0, feat_s.length - 1)
        }
      }
      IO.write(pregap_fp, feat_lines)
    }
  }

  def collect_order_denom(ord: OrderAbs, fs: collection.mutable.Map[(Int,Int),Double])
  def collect_order_numer(ord: OrderAbs, fs: collection.mutable.Map[(Int,Int),Double])
  def cal_pre_gap(orders: Array[OrderAbs], t_len: Int) ={
    val tid_len = 144
    val denom= collection.mutable.Map[(Int, Int), Double]()
    val numer= collection.mutable.Map[(Int, Int), Double]()

    orders.foreach( collect_order_denom( _, denom))
    orders.foreach( collect_order_numer( _, numer))

    Range(1, ditech16.max_time_id + 1 ).map{
      tid =>
         val dids =  districts.values.toArray.sorted
         val arr = new Array[Double](dids.length)

        dids.foreach{
          did =>
            val n = numer.getOrElse((did, tid), 0.0)
            val d = math.max(1.0, denom.getOrElse((did,tid), 1.0 ))
            arr( did - 1) = n / d
        }
        (tid, arr)
    }.toMap

  }

}