package com.houjp.ditech16.datastructure

import com.houjp.common.io.IO

class TimeSlice(val time_slice: String,
                val year: Int,
                val month: Int,
                val day: Int,
                val date: String,
                val time_id: Int,
                val new_time_id: Int) extends Serializable {

  override def toString: String = {
    s"$time_slice"
  }
}

object TimeSlice {

  /**
    * Parse String from old format (without new_time_id field)
    * @param s
    * @return
    */
  def parse_old(s: String): TimeSlice = {
    val y = s.substring(0, 4).toInt
    val m = s.substring(5, 7).toInt
    val d = s.substring(8, 10).toInt
    val date = s.substring(0, 10)
    val tid = s.substring(11).toInt
    val ntid = (tid - 1) * 10 + 1
    val ts = f"$y%04d-$m%02d-$d%02d-$ntid%d"

    new TimeSlice(ts, y, m, d, date, tid, ntid)
  }

  /**
    * Parse String from new format (with new_time_id field)
    * @param s
    * @return
    */
  def parse_new(s: String): TimeSlice = {
    val y = s.substring(0, 4).toInt
    val m = s.substring(5, 7).toInt
    val d = s.substring(8, 10).toInt
    val date = s.substring(0, 10)
    val ntid = s.substring(11).toInt
    val ts = f"$y%04d-$m%02d-$d%02d-$ntid%d"

    new TimeSlice(ts, y, m, d, date, (ntid - 1) / 10 + 1, ntid)
  }


  /**
    * Load time_slices file from disk with old format.
    *
    * @param fp
    * @return
    */
  def load_old(fp: String): Array[TimeSlice] = {
    IO.load(fp).map(parse_old)
  }

  def load_new(fp: String): Array[TimeSlice] = {
    IO.load(fp).map(parse_new)
  }
}

