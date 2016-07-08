package ditech.feature

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District
import ditech.common.util.Directory
import ditech.datastructure.TrafficAbs

object FTrafficJamRate {

  def main(args: Array[String]) {
    // 寻找往前 pre 个时间片的arrive
    run(ditech16.data_pt, this.getClass.getSimpleName.replace("$",""))
  }


  def run(data_pt: String, f_name:String): Unit = {
    val districts_fp = data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

    val date_fp = data_pt + "/dates"
    val dates = IO.load(date_fp).distinct

    val date_traffics = dates.map{ date =>
      val traffic_fp = data_pt + s"/traffic_data/traffic_data_$date"
      val traffic_abs = TrafficAbs.load_local(traffic_fp, districts)

     val traffic_now = cal_pre_traffic(traffic_abs, 1)
      (date, traffic_now)
    }

    val tfc_maxs = date_traffics.flatMap{
      case (date, traffic_now) =>
       val tfc = traffic_now.map{
         case (key, value) =>
           (key._1, value._2 )
       }.toArray
        tfc
    }.groupBy( _._1 ).map{
      group =>
        val did = group._1
        val max = group._2.map{
          case (did,value) =>
            value
        }.max
        (did, max.toDouble)
    }

    date_traffics.foreach {
      case (date, traffic_now) =>
        val preTraffic_dir = data_pt + s"/fs/${f_name}"
        Directory.create(preTraffic_dir)
        val preTraffic_fp = preTraffic_dir + s"/${f_name}_$date"

        val preTraffic_s = districts.values.toArray.sorted.flatMap { did =>
          Range(1, 145).map { tid =>
            val max = math.max( tfc_maxs.getOrElse(did, 1.0), 1.0)

            val (level, total) = traffic_now.getOrElse((did, tid), ((0.0, 0.0, 0.0, 0.0),0.0))
            s"$did,$tid\t${level._1/max},${level._2/max},${level._3/max},${level._4/max}"
          }
        }
        IO.write(preTraffic_fp, preTraffic_s)
    }
  }

  def cal_pre_traffic(traffics: Array[TrafficAbs], t_len: Int): Map[(Int, Int), ((Double,Double,Double,Double),Double)] = {
    val fs = collection.mutable.Map[(Int, Int), ((Double,Double,Double,Double),Double)]()

    traffics.groupBy( x=> (x.did,x.tid) ).foreach{
      group =>
        val did = group._1._1
        var tid = ( group._1._2 + t_len ) % 144
        if( tid == 0 ) tid = 144


        val traffic_level: (Double, Double, Double, Double) = group._2.map{ e =>
           (e.level1.toDouble, e.level2.toDouble, e.level3.toDouble, e.level4.toDouble)
        }.reduce{
          (x,y) =>
            (x._1 + y._1, x._2 + y._2 , x._3 + y._3, x._4 + y._4)
        }

        if( tid == 54 && did == 51  ) println( s"sum: ${traffic_level}" )
        val len = group._2.length
        val level =  ( traffic_level._1 / len,
          traffic_level._2 / len,
          traffic_level._3 /len,
          traffic_level._4 /len )

        fs( (did, tid)) = (level,
          level._1 + level._2 + level._3 + level._4)
    }

    fs.toMap
  }
}