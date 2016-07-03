package com.houjp.ditech16.datastructure

import com.houjp.common.io.IO

/**
  * Abstract class of [[Order]] to speed up the computation.
  *
  * In [[OrderAbs]], do not store order_id, passenger_id, driver_id, district_id.
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
               val new_time_id: Int) extends Serializable {


  override def toString: String = {
    s"$has_driver $start_district_id $dest_district_id $price $year $month $day $hour $minute $second $time_id $new_time_id"
  }
}

object OrderAbs {

  def parse(s: String): OrderAbs = {
    val Array(has_d_s, sdi_s, ddi_s, p_s, y_s, m_s, d_s, hh_s, mm_s, ss_s, ti_s, new_tid_s) = s.split(" ")

    new OrderAbs("false" != has_d_s,
      sdi_s.toInt,
      ddi_s.toInt,
      p_s.toDouble,
      y_s.toInt,
      m_s.toInt,
      d_s.toInt,
      hh_s.toInt, mm_s.toInt,ss_s.toInt,
      ti_s.toInt, new_tid_s.toInt)
  }

  def load_local(fp: String): Array[OrderAbs] = {
    IO.load(fp).map(parse)
  }

  def parse_order(s: String, districts_map: Map[String, Int]): OrderAbs = {
    val Array(oid, did, pid, sd, dd, p_s, t) = s.split("\t")
    val y = t.substring(0, 4).toInt
    val m = t.substring(5, 7).toInt
    val d = t.substring(8, 10).toInt
    val hh = t.substring(11,13).toInt
    val mm = t.substring(14,16).toInt
    val ss = t.substring(17).toInt
    val tid = (hh * 60 + mm) / 10 + 1
    val new_tid = hh * 60 + mm + 1

    new OrderAbs("NULL" != did,
      districts_map.getOrElse(sd, -1),
      districts_map.getOrElse(dd, -1),
      p_s.toDouble, y, m, d, hh, mm, ss, tid, new_tid)
  }

  def load_order_local(fp: String, districts_map: Map[String, Int]): Array[OrderAbs] = {
    IO.load(fp).map(e => parse_order(e, districts_map))
  }
}