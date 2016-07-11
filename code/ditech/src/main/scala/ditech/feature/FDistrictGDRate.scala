package ditech.feature

import com.houjp.ditech16
import ditech.datastructure.OrderAbs
import ditech.feature.ftrait.FDistrictRateTrait

object FDistrictGDRate extends  FDistrictRateTrait{

  def main(args: Array[String]) {
    // 寻找往前 pre 个时间片的gap
    run(ditech16.data_pt,this.getClass.getSimpleName.replace("$","") )
  }

  override def collect_order_numer( ord: OrderAbs, fs: collection.mutable.Map[(Int,Int),Double]) {
    val tid = (ord.fine_time_id + ditech16.max_time_id + 2) / 2
    if (-1 != ord.start_district_id &&
      !ord.has_driver) {
      fs((ord.start_district_id, tid)) = fs.getOrElse((ord.start_district_id, tid), 0.0) + 1.0
    }
  }
  override def collect_order_denom( ord: OrderAbs, fs: collection.mutable.Map[(Int,Int),Double]) {
    val tid = (ord.fine_time_id + ditech16.max_time_id + 2) / 2
    if (-1 != ord.start_district_id ) {
      fs((ord.start_district_id, tid)) = fs.getOrElse((ord.start_district_id, tid), 0.0) + 1.0
    }
  }

}