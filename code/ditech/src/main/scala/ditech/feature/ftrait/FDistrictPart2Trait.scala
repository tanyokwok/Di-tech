package ditech.feature.ftrait

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District
import ditech.common.util.Directory
import ditech.datastructure.OrderAbs

trait FDistrictPart2Trait {
  val districts_fp = ditech16.data_pt + "/cluster_map/cluster_map"
  val districtsType = District.loadDidTypeId(districts_fp)
  val districts = districtsType.mapValues( _._1 )

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

  def collect_order( ord: OrderAbs, fs: collection.mutable.Map[(Int,Int),Double])
  def cal_pre_gap(orders: Array[OrderAbs], t_len: Int) ={
    val tid_len = 144
    val fs = collection.mutable.Map[(Int, Int), Double]()

    orders.foreach( collect_order( _, fs ))

    Range(1, ditech16.max_time_id + 1 ).map{
      tid =>
         val didPart2 =  districtsType.values.toArray.filter{
          case (did, tp) =>
            tp == 2 || tp == 0
         }.map(_._1 ).zipWithIndex

        val arr = new Array[Double](didPart2.length)

        didPart2.foreach{
          case (did,index) =>
            val v = fs.getOrElse((did, tid), 0.0)
            arr( index ) = v
        }
        (tid, arr)
    }.toMap

  }

}