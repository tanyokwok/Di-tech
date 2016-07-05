package ditech.feature.ftrait

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.{TimeUnit, Executors}

import com.houjp.common.Log
import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{District, OrderAbs}
import ditech.common.util.Directory

trait MiniTrait extends Log {
 
  val districts_fp = ditech16.data_pt + "/cluster_map/cluster_map"
  val districts = District.load_local(districts_fp)
  val date_fp = ditech16.data_pt + "/dates"
  val dates = IO.load(date_fp).distinct

  def pre_order_collect( e: OrderAbs, fs: collection.mutable.Map[(Int,Int),Double], off:Int)

  def now_order_collect( e: OrderAbs, fs: collection.mutable.Map[(Int,Int),Double], off:Int)

  class Handler(pregap_dir:String,
                feat_name:String,
                now_date:String) extends Runnable{
    override def run(): Unit ={
      val pregap_fp = pregap_dir + s"/${feat_name}_$now_date"

      val now_order_abs_fp = ditech16.data_pt + s"/order_abs_data/order_data_$now_date"
      val now_orders_abs = OrderAbs.load_local(now_order_abs_fp)

      val date_formate = new SimpleDateFormat("yyyy-MM-dd")
      val cal = Calendar.getInstance()
      cal.setTime(date_formate.parse(now_date))
      cal.set(Calendar.DATE, cal.get(Calendar.DATE) - 1)
      val pre_date = date_formate.format(cal.getTime)

      val pre_order_abs_fp = ditech16.data_pt + s"/order_abs_data/order_data_$pre_date"
      val pre_orders_abs = OrderAbs.load_local(pre_order_abs_fp)

      log(s"pre_date($pre_date),now_date($now_date)")

      val arrive_arr = Range(0,20).map{
        start =>
        cal_pre_gap(pre_orders_abs, now_orders_abs,start)
      }

      val pregap_s = districts.values.toArray.sorted.flatMap { did =>
        Range(1, 1440 + 1).map { ntid =>
         val feat_s = new StringBuffer(s"$did,$ntid\t")
          Range(0,20).foreach{
            idx =>
             val v = arrive_arr(idx).getOrElse((did,ntid), 0.0)
              feat_s.append(s"$v,")
          }
          feat_s.substring(0, feat_s.length() - 1 )
        }
      }
      IO.write(pregap_fp, pregap_s) 
    }
  }
  
  def run(feat_name:String): Unit = {
   
    val pregap_dir = ditech16.data_pt + s"/feature/${feat_name}"
    Directory.create( pregap_dir )

    val threadPool = Executors.newFixedThreadPool(7)
    dates.foreach { now_date =>
      threadPool.execute( new Handler(pregap_dir, feat_name ,now_date ))
    }

    threadPool.shutdown()

    while( true && !threadPool.awaitTermination(10,TimeUnit.SECONDS)){
      println("Please waiting...")
    }


  }

  def cal_pre_gap(pre_orders: Array[OrderAbs], now_orders: Array[OrderAbs], off: Int): Map[(Int, Int), Double] = {
    val fs = collection.mutable.Map[(Int, Int), Double]()

    pre_orders.foreach( pre_order_collect(_, fs, off))
    now_orders.foreach( now_order_collect(_, fs, off))
    fs.toMap
  }

}