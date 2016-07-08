package ditech.feature

import com.houjp.ditech16
import ditech.datastructure.OrderAbs
import ditech.feature.ftrait.FDateDistrictTrait

object FDateDistrictGapNew extends FDateDistrictTrait {

  def main(args:Array[String]): Unit ={
    run(ditech16.data_pt,this.getClass.getSimpleName.replace("$",""))
  }

  override def collect_order( ord:OrderAbs, fs:collection.mutable.Map[Int,Double]): Unit = {
    if (-1 != ord.start_district_id && !ord.has_driver) {
      fs(ord.start_district_id) =
        fs.getOrElse((ord.start_district_id), 0.0) + 1.0
    }
  }
}
