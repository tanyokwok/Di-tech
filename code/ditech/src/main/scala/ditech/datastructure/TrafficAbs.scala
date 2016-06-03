package ditech.datastructure

import com.houjp.common.io.IO
import com.houjp.ditech16.datastructure.District

class TrafficAbs(val did: Int,
                 val level1: Int,
                 val level2: Int,
                 val level3: Int,
                 val level4: Int,
                 val year: Int,
                 val month: Int,
                 val day: Int,
                 val hour: Int,
                 val minute: Int,
                 val second: Int,
                 val tid: Int
             ) extends Serializable {

  override def toString: String = {
      s"did($did)," +
      s"level1($level1)," +
      s"level2($level2)," +
      s"level3($level3)," +
      s"level4($level4)" +
      s"year($year),month($month),day($day),hour($hour),minute($minute),second($second),tid($tid)"
  }
}

object TrafficAbs {

  def parse(s: String, districts_map: Map[String, Int]): TrafficAbs = {
    val Array(dhash, l1, l2, l3, l4, t) = s.split("\t")
    val level1 = l1.substring(2, l1.length ).toInt
    val level2 = l2.substring(2, l2.length ).toInt
    val level3 = l3.substring(2, l3.length ).toInt
    val level4 = l4.substring(2, l4.length ).toInt
    val y = t.substring(0, 4).toInt
    val m = t.substring(5, 7).toInt
    val d = t.substring(8, 10).toInt
    val hh = t.substring(11,13).toInt
    val mm = t.substring(14,16).toInt
    val ss = t.substring(17).toInt
    val tid = (hh * 60 + mm) / 10 + 1

    val did = districts_map.getOrElse( dhash, -1 )
    val traffic = new TrafficAbs(did, level1,level2,level3,level4, y, m, d, hh, mm, ss, tid)
//    if( math.random < 0.1 ){
//     println(s)
//     println( traffic)
//
//    }
   traffic
  }

  def load_local(fp: String, districts_map: Map[String, Int]): Array[TrafficAbs] = {
    IO.load(fp).map(e => parse(e, districts_map))
  }

  def main(args:Array[String]) {
    val districts_fp = ditech.s1_pt + "/cluster_map/cluster_map"
    val districts = District.load_local (districts_fp)

    val traffic_fp = ditech.s1_pt + s"/traffic_data/traffic_data_2016-01-01"
    val traffic_abs = TrafficAbs.load_local (traffic_fp, districts)
  }
}