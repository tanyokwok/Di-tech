package ditech.feature

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District
import ditech.common.util.Directory
import ditech.datastructure.OrderAbs

object FPrefixArriveSelf {
 val districts_fp = ditech16.data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

  def main(args: Array[String]) {
    // 寻找往前 pre 个时间片的gap
    run(ditech16.data_pt,this.getClass.getSimpleName.replace("$",""))
  }

  def run(data_pt: String,f_name:String): Unit = {

    val date_fp = data_pt + "/dates"
    val dates = IO.load(date_fp).distinct

    val pregap_dir = data_pt + s"/fs/${f_name}"
    Directory.create( pregap_dir )
    dates.foreach { date =>
      val order_abs_fp = data_pt + s"/order_data/order_data_$date"
      val orders_abs = OrderAbs.load_local(order_abs_fp,districts)

      val min_fs = cal_minite_gap(orders_abs)
      val pregap_fp = pregap_dir + s"/${f_name}_$date"
      val arrive_map = Range(1,16).map{
        pre =>
          (pre, cal_pre_gap(min_fs, pre*2))
      }.toMap

      val pregap_s = districts.values.toArray.sorted.flatMap { did =>
        Range(1, 145).map { tid =>
          val feats = new StringBuilder(s"$did,$tid\t")
          Range(1,16).foreach{
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

  def cal_minite_gap( orders: Array[OrderAbs]): Map[(Int, Int), Double] ={
    val fs = collection.mutable.Map[(Int, Int), Double]()
    orders.foreach{
      e=>
       if (-1 != e.dest_district_id && e.dest_district_id == e.start_district_id && e.has_driver){
        fs((e.dest_district_id, e.min_time_id)) =
          fs.getOrElse((e.dest_district_id, e.min_time_id), 0.0) + 1.0
      }
    }
    fs.toMap
  }
  def cal_pre_gap(min_fs:Map[(Int,Int),Double], t_len: Int): Map[(Int, Int), Double] = {
    val tid_len = 144
    val fs = collection.mutable.Map[(Int, Int), Double]()

    districts.values.toArray.distinct.sorted.foreach{
      did =>
        Range(1,145).foreach{
          tid =>
            Range(0,t_len).foreach{
              x =>
                //tid = ( hh*60 + mm )/10 + 1
                // min_id = (hh*60 + mm ) + 1
                val min_id = (tid - 1 )*10 - x
                fs( (did,tid) ) =
                  fs.getOrElse( (did,tid), 0.0 ) + min_fs.getOrElse((did,min_id), 0.0 )
            }
        }
    }
    fs.toMap
  }

}