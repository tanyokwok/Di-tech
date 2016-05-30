package com.houjp.ditech16.datastructure

import com.houjp.common.io.IO

class Order(val order_id: String,
            val driver_id: String,
            val passenger_id: String,
            val start_district_hash: String,
            val dest_district_hash: String,
            val start_district_id: Int,
            val dest_district_id: Int,
            val price: Double,
            val time: String,
            val year: Int,
            val month: Int,
            val day: Int,
            val hour: Int,
            val minute: Int,
            val second: Int,
            val time_id: Int) extends Serializable {

  override def toString: String = {
    s"order_id($order_id)," +
      s"driver_id($driver_id)," +
      s"passenger_id($passenger_id)," +
      s"start_district_hash($start_district_hash)," +
      s"dest_district_hash($dest_district_hash)," +
      s"start_district_id($start_district_id)," +
      s"dest_district_id($dest_district_id)," +
      s"price($price)" +
      s"time($time)," +
      s"year($year),month($month),day($day),hour($hour),minute($minute),second($second)," +
      s"time_id($time_id)"
  }
}

object Order {

  def parse(s: String, districts_map: Map[String, Int]): Order = {
    val Array(oid, did, pid, sd, dd, p_s, t) = s.split("\t")
    val y = t.substring(0, 4).toInt
    val m = t.substring(5, 7).toInt
    val d = t.substring(8, 10).toInt
    val hh = t.substring(11,13).toInt
    val mm = t.substring(14,16).toInt
    val ss = t.substring(17).toInt
    val tid = (hh * 60 + mm) / 10 + 1

    new Order(oid, did, pid, sd, dd, districts_map.getOrElse(sd, -1), districts_map.getOrElse(dd, -1), p_s.toDouble, t, y, m, d, hh, mm, ss, tid)
  }

  def load_local(fp: String, districts_map: Map[String, Int]): Array[Order] = {
    IO.load(fp).map(e => parse(e, districts_map))
  }

}