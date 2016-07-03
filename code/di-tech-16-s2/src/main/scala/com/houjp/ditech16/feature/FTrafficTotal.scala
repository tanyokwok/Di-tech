package com.houjp.ditech16.feature

import java.text.SimpleDateFormat
import java.util.Calendar

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District
import ditech.common.util.Directory
import ditech.datastructure.TrafficAbs

object FTrafficTotal {

  def main(args: Array[String]) {
    // 寻找往前 pre 个时间片的arrive
    run(ditech16.data_pt, this.getClass.getSimpleName.replace("$",""))
  }


  def run(data_pt: String, f_name:String): Unit = {
    val districts_fp = data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

    val date_fp = data_pt + "/dates"
    val dates = IO.load(date_fp).distinct

    dates.foreach { now_date =>
      val now_traffic_fp = data_pt + s"/traffic_data/traffic_data_$now_date"
      val now_traffic_abs = TrafficAbs.load_local(now_traffic_fp,districts)

      val date_formate = new SimpleDateFormat("yyyy-MM-dd")
      val cal = Calendar.getInstance()
      cal.setTime(date_formate.parse(now_date))
      cal.set(Calendar.DATE, cal.get(Calendar.DATE) - 1)
      val pre_date = date_formate.format(cal.getTime)

      val pre_traffic_fp = data_pt + s"/traffic_data/traffic_data_$pre_date"
      val pre_traffic_abs = TrafficAbs.load_local(pre_traffic_fp,districts)

      val preTraffic_dir= data_pt + s"/feature/${f_name}"
      Directory.create( preTraffic_dir )
      val preTraffic_fp = preTraffic_dir + s"/${f_name}_$now_date"

      val traffic1 = cal_pre_traffic(pre_traffic_abs, now_traffic_abs, 1)
      val traffic2 = cal_pre_traffic(pre_traffic_abs, now_traffic_abs, 2)
      val traffic3 = cal_pre_traffic(pre_traffic_abs, now_traffic_abs, 3)

      val preTraffic_s = districts.values.toArray.sorted.flatMap { did =>
        Range(1, 1440 + 1).map { tid =>
          val v1 = traffic1.getOrElse((did, tid), 0.0 )
          val v2 = traffic2.getOrElse((did, tid), 0.0 )
          val v3 = traffic3.getOrElse((did, tid), 0.0 )
          s"$did,$tid\t${v1},${v2},${v3}"
        }
      }
      IO.write(preTraffic_fp, preTraffic_s)
    }
  }

  def cal_pre_traffic(pre_traffics: Array[TrafficAbs], now_traffics: Array[TrafficAbs], t_len: Int): Map[(Int, Int),Double] = {
    val tid_len = 144
    val fs = collection.mutable.Map[(Int, Int), Double]()

    pre_traffics.groupBy( x => (x.did,x.tid) ).foreach{
      group =>
        val did = group._1._1
        val tid = group._1._2 + t_len

        if (tid > 144) {
          val x: Double = group._2.map{ e =>
            e.level1.toDouble+ e.level2.toDouble+ e.level3.toDouble+ e.level4.toDouble
          }.sum
          val len = group._2.length
          Range(0, 10).foreach { off =>
            val ntid = (tid - 144 - 1) * 10 + 1 + off
            fs ((did, ntid)) = x / len.toFloat
          }
        }
    }

    now_traffics.groupBy( x => (x.did,x.tid) ).foreach{
      group =>
        val did = group._1._1
        val tid = group._1._2 + t_len

        if (tid <= 144) {
          val x: Double = group._2.map{ e =>
            e.level1.toDouble+ e.level2.toDouble+ e.level3.toDouble+ e.level4.toDouble
          }.sum
          val len = group._2.length
          Range(0, 10).foreach { off =>
            val ntid = (tid - 1) * 10 + 1 + off
            fs ((did, ntid)) = x / len.toFloat
          }
        }
    }

    fs.toMap
  }
}