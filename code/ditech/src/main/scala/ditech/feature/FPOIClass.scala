package ditech.feature

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District
import ditech.common.util.Directory
import ditech.datastructure.POI

object FPOIClass {

  def main(args:Array[String]): Unit ={
    run(ditech16.data_pt, FPOIClass.getClass.getSimpleName.replace("$",""))
  }
  def run( data_pt:String, f_name:String ): Unit ={
    val districts_fp = data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)
    val date_fp = data_pt + "/dates"
    val dates = IO.load(date_fp).distinct
    val poi_fp = data_pt + "/poi_data/poi_data"
    val pois_class = POI.loadKmeans()
    dates.foreach{
      date =>
        val poi_dir = data_pt + s"/fs/${f_name}"
        Directory.create( poi_dir)
        val poi_fp = poi_dir + s"/${f_name}_$date"

        val feats = districts.values.toArray.sorted.flatMap { did =>
          Range(1, 145).map {
            tid =>
              val clazz = pois_class.getOrElse(did,0)
              s"$did,$tid\t$clazz"
          }
        }

        IO.write(poi_fp, feats )
    }
  }

}
