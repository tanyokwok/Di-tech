package ditech.preprocess

import com.houjp.common.io.IO
import com.houjp.ditech16.datastructure.Ans

import scala.collection.mutable.ArrayBuffer


object WeightedSum2 {

  case class Params(path:String ="", count:Int = 0)

  def main(args: Array[String]) {

    val data_pt = "E:/Di-tech/data/offline_compare/"

    run( data_pt)
  }

  def run(data_pt:String) {

    val ans_fp = ArrayBuffer[String](data_pt + "val_pred.csv",data_pt + "ans_test.csv")

    val weighted_sum = collection.mutable.Map[(Int, String), Array[Double]]()
    val weighted_sum_fp = s"$data_pt/ans_ws.csv"
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
          weighted_sum.getOrElse((e.district_id, e.time_slice.time_slice), Array[Double]()) ++ Array(e.gap )
      }
    }

    IO.write(weighted_sum_fp, weighted_sum.toArray.map(e => s"${e._1._1},${e._1._2},${e._2.sum/ e._2.size}"))
  }
}