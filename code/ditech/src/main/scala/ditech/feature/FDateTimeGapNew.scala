package ditech.feature

import com.houjp.ditech16
import ditech.datastructure.OrderAbs
import ditech.feature.ftrait.FDateTimeTrait

object FDateTimeGapNew extends FDateTimeTrait{

  def main(args:Array[String]): Unit ={
    run(ditech16.data_pt,this.getClass.getSimpleName.replace("$",""))
  }

  override def collect_order( ord:OrderAbs, fs:collection.mutable.Map[Int,Double]) {
    if (-1 != ord.start_district_id && !ord.has_driver) {
      fs(ord.time_id) =
        fs.getOrElse(ord.time_id, 0.0) + 1.0
    }
  }
}
