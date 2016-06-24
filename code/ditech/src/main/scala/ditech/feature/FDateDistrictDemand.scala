package ditech.feature

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District
import ditech.common.util.Directory
import ditech.datastructure.OrderAbs
import org.saddle.Vec

object FDateDistrictDemand {

  val districts_fp = ditech16.data_pt + "/cluster_map/cluster_map"
  val districtsType = District.loadDidTypeId(districts_fp)
  val districts = districtsType.mapValues( _._1 )

  def main(args:Array[String]): Unit ={
    run(ditech16.data_pt,this.getClass.getSimpleName.replace("$",""))
  }

  val stat_map = getStatisticsByDate()
  def getStatisticsByDate() ={
    val dates_arr = IO.load(ditech16.data_pt + "/overview_dates").map{
      line =>
        val Array(date,type_s) = line.split("\t")
        (date, type_s.toInt)
    }

    val gaps_map = collection.mutable.Map[Int, Array[Double]]()
    dates_arr.foreach{
     case (date_str, type_id)=>

        val orders = OrderAbs.load_local( ditech16.data_pt + s"/order_data/order_data_$date_str",districts )
        val fs = collection.mutable.Map[Int, Double]()

        orders.foreach { ord =>
          if (-1 != ord.start_district_id ) {
            fs(ord.start_district_id) =
              fs.getOrElse(ord.start_district_id,0.0) + 1.0
          }

        }
        districtsType.values.toArray.filter{
          case (did, tp) =>
            tp == type_id || tp == 0
        }.foreach{
          case (did,tp)=>
            gaps_map( did ) = gaps_map.getOrElse(did, Array[Double]()) ++ Array(fs.getOrElse(did,0.0))
        }

    }
    gaps_map.mapValues{
      fs =>
        val fs_vec = Vec( fs )
        (fs_vec.mean, fs_vec.median, fs_vec.stdev,fs_vec.min.getOrElse(0.0), fs_vec.max.getOrElse(0.0))
//        (fs_vec.mean, fs_vec.median, fs_vec.stdev )
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
          val f = stat_map.getOrElse(did,(0,0,0,0,0))
          Range(1, 145).map {
            tid =>
              s"$did,$tid\t${f._1},${f._2},${f._3},${f._4},${f._5}"
          }
        }

        IO.write(feat_fp, feats )
    }
  }

}
