package ditech.feature

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District
import ditech.common.util.Directory
import ditech.datastructure.OrderAbs

import scala.collection.mutable

object FDistrict {

  def main(args: Array[String]) {
    // 寻找往前 pre 个时间片的gap
    run(ditech16.data_pt,FDistrict.getClass.getSimpleName.replace("$",""))
  }

  def run(data_pt: String, feat_name:String): Unit = {
    val districts_fp = data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

    val date_fp = data_pt + "/dates"
    val dates = IO.load(date_fp).distinct

      val pregap_dir = data_pt + s"/fs/${feat_name}"
      Directory.create( pregap_dir )
    dates.foreach { date =>
      val order_abs_fp = data_pt + s"/order_data/order_data_$date"
      val orders_abs = OrderAbs.load_local(order_abs_fp,districts)

      val pregap_fp = pregap_dir + s"/${feat_name}_$date"

      val feat_map = Range(1,4).map{
        pre =>
          val feat = cal_pre_gap(orders_abs, pre)
          (pre, feat)
      }.toMap

      val pregap_s = districts.values.toArray.sorted.flatMap { did =>
        Range(1, 145).map { tid =>
          val builder = new StringBuilder(s"$did,$tid\t")
          Range(1,4).foreach {
            pre =>
              val feats = feat_map(pre)
              feats.foreach {
                map =>
                  val v = map.getOrElse((did, tid), 0.0)
                  builder.append(s"${v},")
              }

          }

          builder.substring(0, builder.length - 1 )
        }
      }
      IO.write(pregap_fp, pregap_s)
    }
  }

  def cal_pre_gap(orders: Array[OrderAbs], t_len: Int): Array[mutable.Map[(Int, Int), Double]] = {
    val tid_len = 144
    val fs: Array[mutable.Map[(Int, Int), Double]] = Range(0,13).map{
      x =>
        collection.mutable.Map[(Int, Int), Double]()
    }.toArray

    orders.foreach { e =>
      val tid = e.fine_time_id + t_len
      if (tid_len >= tid && 1 <= tid) {

        if (-1 != e.start_district_id) {
          if (!e.has_driver) {
            if (e.dest_district_id == e.start_district_id)
              fs(0)((e.start_district_id, tid)) = fs(0).getOrElse((e.start_district_id, tid), 0.0) + 1.0
            else
              fs(1)((e.start_district_id, tid)) = fs(1).getOrElse((e.start_district_id, tid), 0.0) + 1.0

            fs(2)((e.start_district_id, tid)) = fs(2).getOrElse((e.start_district_id, tid), 0.0) + 1.0
          } else {
            if (e.dest_district_id == e.start_district_id)
              fs(3)((e.start_district_id, tid)) = fs(3).getOrElse((e.start_district_id, tid), 0.0) + 1.0
            else
              fs(4)((e.start_district_id, tid)) = fs(4).getOrElse((e.start_district_id, tid), 0.0) + 1.0
            fs(5)((e.start_district_id, tid)) = fs(5).getOrElse((e.start_district_id, tid), 0.0) + 1.0
          }
          if (e.dest_district_id == e.start_district_id)
            fs(6)((e.start_district_id, tid)) = fs(6).getOrElse((e.start_district_id, tid), 0.0) + 1.0
          else
            fs(7)((e.start_district_id, tid)) = fs(7).getOrElse((e.start_district_id, tid), 0.0) + 1.0
          fs(8)((e.start_district_id, tid)) = fs(8).getOrElse((e.start_district_id, tid), 0.0) + 1.0
        }

        if (-1 != e.dest_district_id) {
          if (e.dest_district_id != e.start_district_id) {
            if (!e.has_driver) {
              fs(9)((e.dest_district_id, tid)) = fs(9).getOrElse((e.dest_district_id, tid), 0.0) + 1.0
            } else {
              fs(10)((e.dest_district_id, tid)) = fs(10).getOrElse((e.dest_district_id, tid), 0.0) + 1.0
            }
            fs(11)((e.dest_district_id, tid)) = fs(11).getOrElse((e.dest_district_id, tid), 0.0) + 1.0
          }

          fs(12)((e.dest_district_id, tid)) = fs(12).getOrElse((e.dest_district_id, tid), 0.0) + 1.0
        }
      }
    }

    fs
  }

}