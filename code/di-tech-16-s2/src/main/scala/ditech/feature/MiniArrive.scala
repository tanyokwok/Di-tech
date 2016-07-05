package ditech.feature

import com.houjp.common.Log
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.OrderAbs
import ditech.feature.ftrait.MiniTrait

object MiniArrive extends MiniTrait with Log {

  def main(args: Array[String]) {
    // 寻找往前 pre 个时间片的gap
    run(this.getClass.getSimpleName.replace("$",""))
  }

  override def pre_order_collect( e: OrderAbs, fs: collection.mutable.Map[(Int,Int),Double], off:Int): Unit = {
    val s_ntid = e.new_time_id + 1 + off
    val e_ntid = e.new_time_id + 10 + off

    val tid_len = ditech16.max_new_time_id
    if (-1 != e.dest_district_id &&
      e.has_driver) {
      Range(tid_len + 1, e_ntid + 1).foreach { ntid =>
        fs((e.dest_district_id, ntid - tid_len)) = fs.getOrElse((e.dest_district_id, ntid - tid_len), 0.0) + 1.0
      }
    }
  }


  override def now_order_collect( e: OrderAbs, fs: collection.mutable.Map[(Int,Int),Double], off:Int): Unit = {
    val s_ntid = e.new_time_id + 1 + off
    val e_ntid = e.new_time_id + 10 + off

    val tid_len = ditech16.max_new_time_id
    if (-1 != e.dest_district_id &&
      e.has_driver
    ) {
      Range(s_ntid, math.min(e_ntid, tid_len) + 1).foreach { ntid =>
        fs((e.dest_district_id, ntid - 0)) = fs.getOrElse((e.dest_district_id, ntid - 0), 0.0) + 1.0
      }
    }
  }

}