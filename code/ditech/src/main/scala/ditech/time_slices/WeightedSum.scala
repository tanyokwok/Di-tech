package ditech.time_slices

import com.houjp.common.io.IO
import com.houjp.ditech16.datastructure.Ans
import scopt.OptionParser

import scala.collection.mutable.ArrayBuffer


object WeightedSum {

  case class Params(path:String ="", count:Int = 0)

  def optionParser(): OptionParser[Params]={
    val parser = new OptionParser[Params]("WeightedSum"){
      opt[String]("path")
        .text("answer_pt")
        .action( (x,c) => c.copy( path = x))
      opt[Int]("count")
        .text("count")
        .action( (x,c) => c.copy( count = x ))
    }
    parser
  }
  def main(args: Array[String]) {
    val parser = optionParser()
    parser.parse(args, new Params()).map{
      params=>
        run(params.path, params.count)
    }.getOrElse(System.exit(-1))
  }

  def run(data_fp: String, count:Int) {

    val ans_fp = ArrayBuffer[String]()

    Range(0, count).foreach { id =>
      val path = s"$data_fp/ans/ans$id.csv"
      println(s"append path: $path")
      ans_fp.append( path)
    }

    val weighted_sum = collection.mutable.Map[(Int, String), Double]()
    val weighted_sum_fp = s"$data_fp/ans/ans_ws.csv"
/*
    val data_fp = ditech16.test1_pt

    val ans_fp = Array(
      s"$data_fp/ans/20160526_fill_seg_rule.csv",
      s"$data_fp/ans/20160526_fill_seg_model.csv")

    val weighted_sum = collection.mutable.Map[(Int, String), Double]()
    val weighted_sum_fp = s"$data_fp/ans/20160526_fill_seg_wsum.csv"
*/
    ans_fp.foreach { fp =>
      val ans = Ans.load(fp)

      ans.foreach { e =>
        weighted_sum((e.district_id, e.time_slice.time_slice)) =
          weighted_sum.getOrElse((e.district_id, e.time_slice.time_slice), 0.0) + e.gap * (1.0 / ans_fp.length)
      }
    }

    IO.write(weighted_sum_fp, weighted_sum.toArray.map(e => s"${e._1._1},${e._1._2},${e._2}"))
  }
}