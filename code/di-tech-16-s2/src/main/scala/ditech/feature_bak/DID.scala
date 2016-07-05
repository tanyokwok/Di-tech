package ditech.feature_bak

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District
import ditech.common.util.Directory

object DID {

  def main(args: Array[String]) {
    run(ditech16.data_pt,  this.getClass.getSimpleName.replace("$","") )
  }

  def run(data_pt: String, f_name:String): Unit = {

    val districts_fp = data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

    val date_fp = data_pt + "/dates"
    val dates = IO.load(date_fp).distinct

    dates.foreach { date =>
      val did_dir = data_pt + s"/fs/$f_name"
      Directory.create( did_dir )
      val did_fp = did_dir + s"/${f_name}_$date"

      val tid_s = districts.values.toArray.sorted.flatMap { did =>
        Range(1, 145).map { tid =>
          s"$did,$tid\t$did"
        }
      }
      IO.write(did_fp, tid_s)
    }
  }
}