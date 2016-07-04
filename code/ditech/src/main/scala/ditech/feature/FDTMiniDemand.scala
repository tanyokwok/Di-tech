package ditech.feature

import com.houjp.ditech16
import ditech.datastructure.OrderAbs
import ditech.feature.ftrait.FDTMiniTrait

object FDTMiniDemand extends FDTMiniTrait{

  def main(args:Array[String]): Unit ={
    run(ditech16.data_pt,this.getClass.getSimpleName.replace("$",""))
  }

  override def collect_order( ord:OrderAbs, fs:collection.mutable.Map[(Int,Int,Int), Double]): Unit ={
    if (-1 != ord.start_district_id) {
      val tid = ord.time_id
      val b_mid = (tid - 1) * 10 + 1
      val mid = ord.min_time_id - b_mid
      fs((ord.start_district_id, tid, mid)) =
        fs.getOrElse((ord.start_district_id, tid, mid), 0.0) + 1.0
    }
  }

}
