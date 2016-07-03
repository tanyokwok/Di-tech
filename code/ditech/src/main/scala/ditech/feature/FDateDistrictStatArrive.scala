package ditech.feature

import ditech.datastructure.OrderAbs
import ditech.feature.ftrait.FDateDistrictStatTrait

object FDateDistrictStatArrive extends FDateDistrictStatTrait{

  def main(args:Array[String]): Unit ={
    run(this.getClass.getSimpleName.replace("$",""))
  }

  def collect_order(ord:OrderAbs,
                    fs:collection.mutable.Map[(Int,Int), Double]): Unit ={
     if (-1 != ord.dest_district_id && ord.has_driver) {
       fs((ord.dest_district_id, ord.time_id)) =
         fs.getOrElse((ord.dest_district_id, ord.time_id), 0.0) + 1.0
     }
  }
}
