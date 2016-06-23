package ditech.feature

import java.util.Calendar

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District
import ditech.common.util.{DateIncrement, Directory}
import ditech.datastructure.OrderAbs
import org.saddle.Vec

object FHolidayTimeGap {

  val districts_fp = ditech16.data_pt + "/cluster_map/cluster_map"
  val districts = District.load_local(districts_fp)

  def main(args:Array[String]): Unit ={
    run(ditech16.data_pt,this.getClass.getSimpleName.replace("$",""))
  }

  val stat_map = getStatisticsByDate("2016-02-23",24)

  def getFeatMap(dates:IndexedSeq[(String,Int)]): collection.Map[Int,(Double, Double, Double, Double, Double)] ={
   //get gaps of every day
   val feat_map = dates.map{
     case (date_str,weekday)=>
       val orders = OrderAbs.load_local( ditech16.data_pt + s"/order_data/order_data_$date_str",districts )

        val fs = collection.mutable.Map[Int, Double]()
        Range(1,145).foreach { tid =>
          fs(tid) = 0
        }
        orders.foreach { ord =>
          if (-1 != ord.start_district_id && !ord.has_driver) {
              fs(ord.time_id) =
                fs(ord.time_id) + 1.0
          }
        }
        fs.mapValues( x => Array(x))
    }.reduce{
      (x,y) =>
        val z = ( x /: y){
          case (map, (k,v)) =>
           map + (k->( map(k) ++ v ))
        }
       z
    }
    feat_map.mapValues{
      fs =>
        val fs_vec = Vec( fs )
        (fs_vec.mean, fs_vec.median, fs_vec.stdev,fs_vec.min.getOrElse(0.0), fs_vec.max.getOrElse(0.0))
//        (fs_vec.mean, fs_vec.median, fs_vec.stdev )
    }
  }
  def getStatisticsByDate(start_date:String, day_count:Int) ={
    val date = DateIncrement(start_date)

    val cld = Calendar.getInstance()

    val dates = Range(0,day_count).map{
     x =>
       val date_str = date.toString
       cld.setTime( date.date )
       val weekday = cld.get(Calendar.DAY_OF_WEEK)
       date.next()
       println( s"$x $date_str $weekday")
       (date_str,weekday)
   }
   val  holiday_dates = dates.filter{
     case (date_str, weekday)=>
       weekday == 1 || weekday == 7 || date_str.equals("2016-01-01")
   }
    val workday_dates = dates.filter{
      case (date_str, weekday) =>
        !(weekday == 1 || weekday == 7 || date_str.equals("2016-01-01"))
    }
    val holiday_feat = getFeatMap(holiday_dates)
    val workday_feat = getFeatMap(workday_dates)

    (holiday_feat, workday_feat)
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
          Range(1, 145).map {
            tid =>
              val f1 = stat_map._1.getOrElse(tid,(0,0,0,0,0))
              val f2 = stat_map._2.getOrElse(tid,(0,0,0,0,0))
              s"$did,$tid\t${f1._1},${f1._2},${f1._3},${f1._4},${f1._5},${f2._1},${f2._2},${f2._3},${f2._4},${f2._5}"
          }
        }

        IO.write(feat_fp, feats )
    }
  }

}
