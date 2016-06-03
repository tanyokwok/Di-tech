package com.houjp.ditech16.rule

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{District, TimeSlice}

object EmptyFiller {
  def main(args: Array[String]) {
    run(ditech16.train_pt)
//    run(ditech16.test1_pt)
  }

  def run(data_pt: String): Unit = {
    val ans_fp = data_pt + "/ans/ans.csv"

    val ans_map = IO.load(ans_fp).map { s =>
      val Array(did, t, gap) = s.split(",")
      ((did.toInt, t), gap.toDouble)
    }.toMap

    val time_slices_fp = data_pt + "/test_time_slices"
    val time_slices: Array[TimeSlice] = TimeSlice.load_local(time_slices_fp)

    val districts_fp = ditech16.s1_pt + "/cluster_map/cluster_map"
    val districts: Map[String, Int] = District.load_local(districts_fp)

    val ans_rule_fp = data_pt + "/ans/ans_rule.csv"

    var ans_rule = ans_map

    ans_rule = seg(ans_map, districts, time_slices)
//    ans_rule = exp(ans_map, districts, time_slices)

    ans_rule = ans_rule.map { e =>
      (e._1, math.max(1.0, e._2))
    }

    IO.write(ans_rule_fp, ans_rule.toArray.map { e =>
      s"${e._1._1},${e._1._2},${e._2}"
    })
  }

  def seg(ans_map: Map[(Int, String), Double],
          districts: Map[String, Int],
          time_slices: Array[TimeSlice]): Map[(Int, String), Double] = {

    districts.values.toArray.sorted.flatMap { did =>
      time_slices.map(_.time_slice).map { t =>
        val raw_gap = ans_map.getOrElse((did, t), 0.0)
        val gap = if (raw_gap > 200) {
          raw_gap * 0.8
        } else if (raw_gap > 100.0) {
          raw_gap * 0.7
        } else if (raw_gap > 30) {
          raw_gap * 0.6
        } else {
          raw_gap * 0.5
        }
        ((did, t), gap)
      }
    }.toMap
  }

  def exp(ans_map: Map[(Int, String), Double],
          districts: Map[String, Int],
          time_slices: Array[TimeSlice]): Map[(Int, String), Double] = {

    districts.values.toArray.sorted.flatMap { did =>
      time_slices.map(_.time_slice).map { t =>
        val raw_gap = ans_map.getOrElse((did, t), 0.0)
        val gap = raw_gap * (1.0 / (1.0 + math.exp(-0.001 * raw_gap)))
        ((did, t), gap)
      }
    }.toMap
  }

  def run_online(): Unit = {
    val ans_fp = ditech16.test1_pt + "/ans/ans.csv"
    //val ans_fp = "/Users/hugh_627/ICT/competition/di-tech-16//data//ans/20160525_pregapave123.csv"

    val ans_map = IO.load(ans_fp).map { s =>
      val Array(did, t, gap) = s.split(",")
      ((did.toInt, t), gap.toDouble)
    }.toMap

    val time_slices_fp = ditech16.test1_time_slices
    val time_slices = TimeSlice.load_local(time_slices_fp)

    val districts_fp = ditech16.test1_cluater_pt + "/cluster_map"
    val districts = District.load_local(districts_fp)

    val pre_gap_fill_fp = ditech16.test1_pt + "/ans/20160526_fill_seg_model.csv"
    //val pre_gap_fill_fp = "/Users/hugh_627/ICT/competition/di-tech-16//data//ans/20160525_pregapave123_fill1_sub6.csv"

    IO.write(pre_gap_fill_fp, districts.values.toArray.sorted.flatMap { did =>
      time_slices.map(_.time_slice).map { t =>
        val raw_gap = ans_map.getOrElse((did, t), 0.0)
        // Plan A:
        // val gap = math.max(1.0, ans_map.getOrElse((did, t), 0.0) * 0.5 - 0.0)

        // Plan B:
        val gap = if (raw_gap > 200) {
          raw_gap * 0.8
        } else if (raw_gap > 100.0) {
          raw_gap * 0.7
        } else if (raw_gap > 30) {
          raw_gap * 0.6
        } else {
          raw_gap * 0.5
        }

        // Plan C:
        // val gap = raw_gap * (1.0 / (1.0 + math.exp(-0.001 * raw_gap)))
        s"$did,$t,${math.max(1.0, gap)}"
      }
    })
  }
}