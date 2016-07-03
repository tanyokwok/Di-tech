package ditech.datastructure

import com.houjp.common.io.IO

/**
  */
class OrderAbs(val has_driver: Boolean,
               val start_district_id: Int,
               val dest_district_id: Int,
               val price: Double,
               val year: Int,
               val month: Int,
               val day: Int,
               val hour: Int,
               val minute: Int,
               val second: Int,
               val time_id: Int,
               val fine_time_id:Int,
               val min_time_id:Int) extends Serializable {


  override def toString: String = {
    s"$has_driver $start_district_id $dest_district_id $price $year $month $day $hour $minute $second $time_id $fine_time_id"
  }

  /*
  override def toString: String = {
    s"has_driver($has_driver)," +
      s"start_district_id($start_district_id)," +
      s"dest_district_id($dest_district_id)," +
      s"price($price)," +
      s"year($year),month($month),day($day),hour($hour),minute($minute),second($second)," +
      s"time_id($time_id)"
  }
  */
}

object OrderAbs {


  def parse_order(s: String, districts_map: Map[String, Int]): OrderAbs = {
    val Array(oid, did, pid, sd, dd, p_s, t) = s.split("\t")
    val y = t.substring(0, 4).toInt
    val m = t.substring(5, 7).toInt
    val d = t.substring(8, 10).toInt
    val hh = t.substring(11,13).toInt
    val mm = t.substring(14,16).toInt
    val ss = t.substring(17).toInt
    val tid = (hh * 60 + mm) / 10 + 1
    val fine_tid = (hh * 60 + mm) / 5 + 1
    val min_tid = (hh*60 + mm) + 1


    new OrderAbs("NULL" != did,
      districts_map.getOrElse(sd, -1),
      districts_map.getOrElse(dd, -1),
      p_s.toDouble, y, m, d, hh, mm, ss, tid, fine_tid,min_tid)
  }

  def load_local(fp: String, districts_map: Map[String, Int]): Array[OrderAbs] = {
    IO.load(fp).map(e => parse_order(e, districts_map))
  }
}