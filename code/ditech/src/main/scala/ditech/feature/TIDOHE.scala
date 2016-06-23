package ditech.feature

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District
import ditech.common.util.Directory

object TIDOHE {

  def main(args: Array[String]) {
    run(ditech16.data_pt,this.getClass.getSimpleName.replace("$",""))
  }

  def run(data_pt: String,f_name:String): Unit = {

    val districts_fp = data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

    val date_fp = data_pt + "/dates"
    val dates = IO.load(date_fp).distinct

    dates.foreach { date =>
      val tid = collection.mutable.Map[(Int, Int), Double]()
      val tid_dir = data_pt + s"/fs/$f_name"
      Directory.create( tid_dir )
      val tid_fp = tid_dir + s"/${f_name}_$date"

      val tid_s = districts.values.toArray.sorted.flatMap { did =>
        Range(1, 145).map { tid =>
          val feat = new StringBuilder(s"$did,$tid\t$tid")
          Range(1,145).foreach{
            id =>
              if( tid == id )  feat.append(s"1,")
              else feat.append(s"0,")
          }
          feat.substring(0,feat.length - 1)
        }
      }
      IO.write(tid_fp, tid_s)
    }
  }
}