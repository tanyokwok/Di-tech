package ditech.feature

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District
import ditech.common.util.Directory
import ditech.datastructure.TrafficAbs

object TrafficFeature {

  def main(args: Array[String]) {
    // 寻找往前 pre 个时间片的arrive
    run(ditech16.s1_pt, 1)
    run(ditech16.s1_pt, 2)
    run(ditech16.s1_pt, 3)
  }


  def run(data_pt: String, pre: Int): Unit = {
    val districts_fp = data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

    val date_fp = data_pt + "/dates"
    val dates = IO.load(date_fp).distinct

    dates.foreach { date =>
      val traffic_fp = data_pt + s"/traffic_data/traffic_data_$date"
      val traffic_abs = TrafficAbs.load_local(traffic_fp,districts)

      val preTraffic_dir= data_pt + s"/fs/traffic_$pre"
      Directory.create( preTraffic_dir )
      val preTraffic_fp = preTraffic_dir + s"/traffic_${pre}_$date"

      val preTraffic = cal_pre_traffic(traffic_abs, pre)

      val preTraffic_s = districts.values.toArray.sorted.flatMap { did =>
        Range(1, 145).map { tid =>
          val v = preTraffic.getOrElse((did, tid), (0,0,0,0) )
          s"$did,$tid\t${v._1},${v._2},${v._3},${v._4}"
        }
      }
      IO.write(preTraffic_fp, preTraffic_s)
    }
  }

  def cal_pre_traffic(traffics: Array[TrafficAbs], t_len: Int): Map[(Int, Int),(Double,Double,Double,Double)] = {
    val tid_len = 144
    val fs = collection.mutable.Map[(Int, Int), (Double,Double,Double,Double)]()

    traffics.groupBy( x=> (x.did,x.tid) ).foreach{
      group =>
        val did = group._1._1
        var tid = ( group._1._2 + t_len ) % 144
        if( tid == 0 ) tid = 144


        val x: (Double, Double, Double, Double) = group._2.map{ e =>
          if( tid == 54 && did == 51 ) println( e)
           (e.level1.toDouble, e.level2.toDouble, e.level3.toDouble, e.level4.toDouble)
        }.reduce{
          (x,y) =>
            (x._1 + y._1, x._2 + y._2 , x._3 + y._3, x._4 + y._4)
        }

        if( tid == 54 && did == 51  ) println( s"sum: ${x}" )
        val len = group._2.length
        fs( (did, tid)) = ( x._1 / len, x._2 / len, x._3 /len, x._4 /len )
    }

    fs.toMap
  }
}