package com.houjp.ditech16.score

import com.houjp.ditech16
import com.houjp.ditech16.datastructure.Ans

object Score {

  def optionParser(args:Array[String]): Unit ={
    val default_params = Params()
    val paser = new OptionParser[Params]("Score"){

    }
  }
  def main(args: Array[String]) {

    val std_fp = ditech16.train_ans_pt + "/std.csv"
    val ans_fp = ditech16.train_ans_pt + "/ans.csv"

    val std = Ans.load(std_fp).filter(_.gap > 1e-6)
    val ans = Ans.load(ans_fp).filter(_.gap > 1e-6)

    println(run(std, ans).formatted("%.6f"))
  }

  def run(std: Array[Ans], ans: Array[Ans]): Double = {
    val std_map = std.map { e =>
      ((e.district_id, e.time_slice.date, e.time_slice.time_id), e.gap)
    }.toMap

    val ans_map = ans.map { e =>
      ((e.district_id, e.time_slice.date, e.time_slice.time_id), e.gap)
    }.toMap

    val district_sum_map = collection.mutable.Map() ++ std.map(_.district_id).distinct.map(e => (e, 0.0)).toMap
    val district_len_map = collection.mutable.Map() ++ std.map(_.district_id).distinct.map(e => (e, 0.0)).toMap

    var score = 0.0

    std_map.foreach { e =>
      // sprintln(s"${e._1}, ${e._2}, ${ans_map.getOrElse(e._1, 0.0)}")
      val std_gap = e._2
      val ans_gap = ans_map.getOrElse(e._1, 0.0)
      val did = e._1._1

      district_sum_map(did) += math.abs((std_gap - ans_gap) / std_gap)
      district_len_map(did) += 1.0
    }

    district_sum_map.foreach { e =>
      println(s"District ID: ${e._1}, District Sum: ${e._2}, District Len: ${district_len_map(e._1)}")
      score += e._2 / district_len_map(e._1)
    }

    score / district_sum_map.size
  }

  case class Params(ans_pt:String="")
}