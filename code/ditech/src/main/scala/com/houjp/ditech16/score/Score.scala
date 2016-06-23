package com.houjp.ditech16.score

import com.houjp.ditech16
import com.houjp.ditech16.datastructure.Ans
import scopt.OptionParser

object Score {

  def optionParser(): OptionParser[Params]={
    val parser = new OptionParser[Params]("Score"){
      opt[String]("std_name")
        .text(s"the name of standard answer")
        .action((x,c) => c.copy( std_name = x ))
      opt[String]("ans_name")
        .text(s"the name of answer")
        .action((x,c) => c.copy( ans_name = x ))
    }
    parser
  }
  def main(args: Array[String]) {

    val default_params = Params()
    val parser = optionParser()
    parser.parse(args, default_params).map{
      params =>
      val std_fp = ditech16.ans_pt + s"/${params.std_name}.csv"
      val ans_fp = ditech16.ans_pt + s"/${params.ans_name}.csv"

        println(s"evaluate answer " + ans_fp)
      val std = Ans.load(std_fp)
      val ans = Ans.load(ans_fp)

      run(std, ans)
    }.getOrElse{
      System.exit(1)
    }

  }

  def run(std: Array[Ans], ans: Array[Ans]) = {
    val std_map = std.map { e =>
      ((e.district_id, e.time_slice.date, e.time_slice.time_id), e.gap)
    }.toMap

    val ans_map = ans.map { e =>
      ((e.district_id, e.time_slice.date, e.time_slice.time_id), e.gap)
    }.toMap

    val district_sum_map = collection.mutable.Map() ++ std.map(_.district_id).distinct.map(e => (e, 0.0)).toMap
    val district_len_map = collection.mutable.Map() ++ std.map(_.district_id).distinct.map(e => (e, 0.0)).toMap


    std_map.foreach { e =>
      // sprintln(s"${e._1}, ${e._2}, ${ans_map.getOrElse(e._1, 0.0)}")
      val std_gap = e._2
      val ans_gap = ans_map.getOrElse(e._1, 0.0)
      val did = e._1._1

      district_sum_map(did) += math.abs(std_gap - ans_gap)
      district_len_map(did) += 1.0
    }

    var score0 = 0.0
    var score1 = 0.0
    district_sum_map.foreach { e =>
//      println(s"District ID: ${e._1}, District Sum: ${e._2}, District Len: ${district_len_map(e._1)}")
      score0 += e._2 / district_len_map(e._1)
    }
    println( "score: "
      + (score0 / district_sum_map.size).formatted("%.6f") )
  }

  case class Params(std_name:String="std",ans_name:String="ans")
}