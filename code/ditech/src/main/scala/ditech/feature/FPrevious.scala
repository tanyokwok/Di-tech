package ditech.feature

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District
import ditech.common.util.Directory
import ditech.datastructure.OrderAbs
import org.saddle.Vec

object FPrevious {
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
      val arrive_map: Map[(Int, Int), Array[Double]] = cal_pre_gap(min_fs, 30)

      val pregap_s = districts.values.toArray.sorted.flatMap { did =>
        Range(1, 145).map { tid =>
          val feats = new StringBuilder(s"$did,$tid\t")
          val stat_arr = arrive_map.getOrElse((did,tid),new Array[Double](30))

          stat_arr.foreach{
            v =>
              feats.append( s"$v,")
          }

          val vec = Vec( stat_arr )
          feats.append( s"${vec.mean},${vec.stdev},${vec.median},${vec.min.getOrElse(0)},${vec.max.getOrElse(0)}")
          feats.toString
        }
      }
      IO.write(pregap_fp, pregap_s)
    }
  }

  def cal_minite_gap( orders: Array[OrderAbs]): Map[(Int, Int), Double] ={
    val fs = collection.mutable.Map[(Int, Int), Double]()
    orders.foreach{
      e=>
       if (-1 != e.dest_district_id && e.has_driver){
        fs((e.dest_district_id, e.min_time_id)) =
          fs.getOrElse((e.dest_district_id, e.min_time_id), 0.0) + 1.0
      }
    }
    fs.toMap
  }
  def cal_pre_gap(min_fs:Map[(Int,Int),Double], window:Int ): Map[(Int, Int), Array[Double]] = {

    districts.values.toArray.distinct.sorted.flatMap{
      did =>
        Range(1,ditech16.max_time_id + 1).map{
          tid =>
            val gaps = Range(0,window).map{
              x =>
                //tid = ( hh*60 + mm )/10 + 1
                //min_id = (hh*60 + mm ) + 1
                val min_id = (tid - 1 )*10 - x
                min_fs.getOrElse((did,min_id), 0.0 )
            }.toArray

            ((did,tid),gaps)
        }
    }.toMap
  }

}