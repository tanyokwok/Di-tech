package com.houjp.ditech16.postprocess

import com.houjp.common.io.IO
import com.houjp.ditech16.datastructure.Ans

object NewAns2Ans {

  def main(args: Array[String]) {
    run(args(0))
  }

  def run(data_fp: String): Unit = {

    val ans = Ans.load_new(data_fp + "/ans.csv")
    IO.write(data_fp + "/ans2.csv", ans.map { e =>
      f"${e.district_id},${e.time_slice.year}-${e.time_slice.month}%02d-${e.time_slice.day}%02d-${e.time_slice.time_id},${e.gap}"
    })
  }
}