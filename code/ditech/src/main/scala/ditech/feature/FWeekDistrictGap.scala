package ditech.feature

import java.text.SimpleDateFormat
import java.util.Calendar

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District
import ditech.common.util.Directory
import ditech.datastructure.OrderAbs
import org.saddle.Vec

object FWeekDistrictGap {

  val districts_fp = ditech16.data_pt + "/cluster_map/cluster_map"
  val districtsType = District.loadDidTypeId(districts_fp)
  val districts = districtsType.mapValues( _._1 )

  def main(args:Array[String]): Unit ={
    run(ditech16.data_pt,this.getClass.getSimpleName.replace("$",""))
  }

  def getFeatMap(dates:IndexedSeq[(String,Int,Int)]): collection.Map[Int,(Double, Double, Double, Double, Double)] ={
    val gaps_map = collection.mutable.Map[Int, Array[Double]]()
   //get gaps of every day
   dates.foreach{
     case (date_str,weekday,type_id)=>
       val orders = OrderAbs.load_local( ditech16.data_pt + s"/order_data/order_data_$date_str",districts )

        val fs = collection.mutable.Map[Int, Double]()

        orders.foreach { ord =>
          if (-1 != ord.start_district_id && !ord.has_driver) {
              fs(ord.start_district_id) =
                fs.getOrElse(ord.start_district_id,0.0) + 1.0
          }
        }
       districtsType.values.toArray.filter{
          case (did, tp) =>
            tp == type_id || tp == 0
        }.foreach{
          case (did,tp)=>
             gaps_map( did) = gaps_map.getOrElse(did, Array[Double]()) ++ Array(fs.getOrElse(did,0.0))
        }

    }
     gaps_map.mapValues{
      fs =>
        val fs_vec = Vec( fs )
        (fs_vec.mean, fs_vec.median, fs_vec.stdev,fs_vec.min.getOrElse(0.0), fs_vec.max.getOrElse(0.0))
    }

  }

  val stat_map = getStatisticsByDate()
  def getStatisticsByDate() ={
    val dates_arr = IO.load(ditech16.data_pt + "/overview_dates").map{
      line =>
        val Array(date,type_s) = line.split("\t")
        (date, type_s.toInt)
    }

    val gaps_map = collection.mutable.Map[(Int,Int), Array[Double]]()
    val cld = Calendar.getInstance()
    val simFormat = new SimpleDateFormat("yyyy-MM-dd")
    dates_arr.map{
      case (date_str, type_id)=>

       cld.setTime( simFormat.parse( date_str ))
       val weekday = cld.get(Calendar.DAY_OF_WEEK)
       (date_str,weekday,type_id)
   }.groupBy( _._2 ).map{
      group =>
        val weekday = group._1
       val feat_map = getFeatMap( group._2 )
        (weekday, feat_map)
    }

  }
  def run( data_pt:String, feat_name:String ): Unit ={
   val date_fp = data_pt + "/dates"
   val dates = IO.load(date_fp).distinct
   val simFormat = new SimpleDateFormat("yyyy-MM-dd")
   dates.foreach{
      date =>
        val feat_dir = data_pt + s"/fs/$feat_name"
        Directory.create( feat_dir)
        val feat_fp = feat_dir + s"/${feat_name}_$date"

        val cld = Calendar.getInstance()
        cld.setTime( simFormat.parse( date ) )
        val weekday = cld.get(Calendar.DAY_OF_WEEK)
        val feats = districts.values.toArray.sorted.flatMap { did =>
           val f1 = stat_map(weekday).getOrElse(did,(0,0,0,0,0))
           Range(1, 145).map {
            tid =>
             s"$did,$tid\t${f1._1},${f1._2},${f1._3},${f1._4},${f1._5}"
          }
        }

        IO.write(feat_fp, feats )
    }
  }

}
