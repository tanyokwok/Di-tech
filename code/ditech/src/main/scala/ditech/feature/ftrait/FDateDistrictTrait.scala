package ditech.feature.ftrait

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District
import ditech.common.util.Directory
import ditech.datastructure.OrderAbs
import org.saddle.Vec

trait FDateDistrictTrait {

  val districts_fp = ditech16.data_pt + "/cluster_map/cluster_map"
  val districtsType = District.loadDidTypeId(districts_fp)
  val districts = districtsType.mapValues( _._1 )

  def collect_order( ord:OrderAbs, fs:collection.mutable.Map[Int,Double])

  val stat_map = getStatisticsByDate()
  def getStatisticsByDate() ={
    val dates_arr = IO.load(ditech16.data_pt + "/overview_dates").map{
      line =>
        val Array(date,type_s) = line.split("\t")
        (date, type_s.toInt)
    }

    val global_stat = collection.mutable.Map[Int, Array[(String,Double)]]()
   //get gaps of every day
    dates_arr.foreach{
     case (date_str, type_id)=>
        val orders = OrderAbs.load_local( ditech16.data_pt + s"/order_data/order_data_$date_str",districts )
        val fs = collection.mutable.Map[Int, Double]()

       orders.foreach( collect_order(_, fs))

        districtsType.values.toArray.filter{
          case (did, tp) =>
            tp == type_id || tp == 0
        }.foreach{
          case (did,tp)=>
            global_stat( did ) = global_stat.getOrElse(did, Array[(String,Double)]()) ++
              Array((date_str,fs.getOrElse(did,0.0)))
        }

    }

    global_stat

  }

  def getFeat(date:String, did:Int) ={
     val smp_arr = stat_map.getOrElse( did, Array[(String,Double)]()).filter( _._1 != date).map( _._2 )
    if( smp_arr.length == 0 ) (0.0,0.0,0.0,0.0,0.0)
    else{
      val fs_vec = Vec(smp_arr)
      (fs_vec.mean, fs_vec.median, fs_vec.stdev, fs_vec.min.getOrElse(0.0), fs_vec.max.getOrElse(0.0))
    }

  }
  def run( data_pt:String, feat_name:String ): Unit ={
   val date_fp = data_pt + "/dates"
   val dates = IO.load(date_fp).distinct
   dates.foreach{
      date =>
        val feat_dir = data_pt + s"/fs/$feat_name"
        Directory.create( feat_dir)
        val feat_fp = feat_dir + s"/${feat_name}_$date"

        val feats = districts.values.toArray.sorted.flatMap { did =>
         val f = getFeat(date,did)
          Range(1, 145).map {
            tid =>
              s"$did,$tid\t${f._1},${f._2},${f._3},${f._4},${f._5}"
          }
        }

        IO.write(feat_fp, feats )
    }
  }

}
