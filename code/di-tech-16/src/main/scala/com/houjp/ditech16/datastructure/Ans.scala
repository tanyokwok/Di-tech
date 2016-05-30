package com.houjp.ditech16.datastructure

import com.houjp.common.io.IO

class Ans(val district_id: Int, val time_slice: TimeSlice, val gap: Double) extends Serializable {

  override def toString: String = {
    s"$district_id,$time_slice,$gap"
  }
}

object Ans {

  def toAns(triple: (Int, TimeSlice, Double)): Ans = {
    new Ans(triple._1, triple._2, triple._3)
  }

  def load(fp: String): Array[Ans] = {
    IO.load(fp).map { s =>
      val Array(did_s, ts_s, gap_s) = s.split(",")
      new Ans(did_s.toInt, TimeSlice.parse(ts_s), gap_s.toDouble)
    }
  }
}