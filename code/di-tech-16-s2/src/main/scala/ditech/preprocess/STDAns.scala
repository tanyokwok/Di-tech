package ditech.preprocess

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{District, TimeSlice}


object STDAns {

  def main(args: Array[String]) {
    run(ditech16.data_pt)
  }

  def run(data_pt: String): Unit = {
    generate_std_ans(ditech16.ans_pt + "/test_std.csv", ditech16.offline_pt + "/test_time_slices")
    generate_std_ans(ditech16.ans_pt + "/val_std1.csv", ditech16.offline_pt + "/val_time_slices1")
    generate_std_ans(ditech16.ans_pt + "/val_std2.csv", ditech16.offline_pt + "/val_time_slices2")
    generate_std_ans(ditech16.ans_pt + "/val_std.csv", ditech16.online_pt + "/val_time_slices")
  }

  def generate_std_ans(ans_fp:String, new_time_slices_fp: String): Unit = {

    // 第2赛季出现的did
    val districts_fp = ditech16.data_pt + "/cluster_map/cluster_map"
    val districts_set = District.loadDidTypeId(districts_fp).filter(e => e._2._2 != 1).map(e => e._2._1).toSet

    val ans = TimeSlice.load_new(new_time_slices_fp).flatMap { ts =>
      val date = f"${ts.year}%04d-${ts.month}%02d-${ts.day}%02d"
      val label_fp = ditech16.data_pt + s"/label/label_$date"
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