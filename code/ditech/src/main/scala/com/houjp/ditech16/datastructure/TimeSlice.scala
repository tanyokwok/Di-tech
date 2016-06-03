package com.houjp.ditech16.datastructure

import com.houjp.common.io.IO

class TimeSlice(val time_slice: String,
                val year: Int,
                val month: Int,
                val day: Int,
                val date: String,
                val time_id: Int) extends Serializable {

  override def toString: String = {
    s"$time_slice"
  }
}

object TimeSlice {

  def parse(s: String): TimeSlice = {
    val y = s.substring(0, 4).toInt
    val m = s.substring(5, 7).toInt
    val d = s.substring(8, 10).toInt
    val date = s.substring(0, 10)
    val tid = s.substring(11).toInt

    new TimeSlice(s, y, m, d, date, tid)
  }

  /**
    * Load time_slices file from disk.
    *
    * @param fp
    * @return
    */
  def load_local(fp: String): Array[TimeSlice] = {
    IO.load(fp).map(parse)
  }
}

