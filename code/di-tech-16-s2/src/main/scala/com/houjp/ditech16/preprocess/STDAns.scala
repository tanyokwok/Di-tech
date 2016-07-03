package com.houjp.ditech16.preprocess

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{District, OrderAbs, TimeSlice}


object STDAns {

  def main(args: Array[String]) {
    run(ditech16.data_pt)
  }

  def run(data_pt: String): Unit = {
    run_offset(data_pt, 0)
    run_offset(data_pt, -10)
    run_offset(data_pt, 10)
  }

  def run_offset(data_pt: String, offset: Int): Unit = {

    // 第2赛季出现的did
    val districts_fp = data_pt + "/cluster_map/cluster_map"
    val districts_set = District.loadDidTypeId(districts_fp).filter(e => e._2._2 != 1).map(e => e._2._1).toSet

    val ans_fp = data_pt + s"/offline/ans/std_$offset.csv"
    val new_time_slices_fp = data_pt + s"/offline/test_new_time_slices_$offset"
    val ans = TimeSlice.load_new(new_time_slices_fp).flatMap { ts =>
      val date = f"${ts.year}%04d-${ts.month}%02d-${ts.day}%02d"
      val label_fp = data_pt + s"/label/label_$date"
      val label = IO.load(label_fp).map { e =>
        val Array(key, ls) = e.split("\t")
        val Array(did, ntid) = key.split(",")
        ((did.toInt, ntid.toInt), ls.toDouble)
      }.toMap
      districts_set.map { did =>
        s"$did,$date-${ts.new_time_id},${label((did, ts.new_time_id))}"
      }
    }
    IO.write(ans_fp, ans)
  }
}