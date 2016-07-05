package ditech.feature

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{OrderAbs, District}
import ditech.common.util.Directory
import ditech.datastructure.Weather
import org.saddle.Vec

object FDTWeatherDistrictGap {

  val districts_fp = ditech16.data_pt + "/cluster_map/cluster_map"
  val districtsType = District.loadDidTypeId(districts_fp)
  val date_fp = ditech16.data_pt + "/dates"
  val dates = IO.load(date_fp).distinct
  val districts = districtsType.mapValues( _._1 )
  val weatIdMap: Map[Int, Int] = dates.flatMap {
    date =>
      val weather_data_fp = ditech16.data_pt + s"/weather_data/weather_data_$date"
      val weat_types = Weather.load_weatherType(weather_data_fp)
      weat_types
  }.distinct.sorted.zipWithIndex.toMap

  def main(args:Array[String]): Unit ={
    run(this.getClass.getSimpleName.replace("$",""))
  }

  val stat_map = getStatisticsByDate()
  def getStatisticsByDate() ={
    //load overivew dates
    val dates_arr = IO.load(ditech16.data_pt + "/overview_dates").map{
      line =>
        val Array(date,type_s) = line.split("\t")
        (date, type_s.toInt)
    }

    //collect and merge all data together by (did,tid,wid)
    val gaps_map = collection.mutable.Map[(Int,Int), Array[Double]]()
    dates_arr.foreach{
     case (date_str, type_id)=>
        val weather_data_fp = ditech16.data_pt + s"/weather_data/weather_data_$date_str"
        val weather_map = Weather.load_map(weather_data_fp)
        val orders = OrderAbs.load_local( ditech16.data_pt + s"/order_data/order_data_$date_str")
        val fs = collection.mutable.Map[(Int, Int), Double]()

        orders.foreach { ord =>
          if (-1 != ord.start_district_id && !ord.has_driver) {
            val tid = ord.time_id
            val weat = weather_map.getOrElse( tid, Weather.fillWeather( weather_map, tid))
            val wid = weatIdMap(weat.weather)
            fs((ord.start_district_id, wid)) =
              fs.getOrElse((ord.start_district_id, wid),0.0) + 1.0
          }
        }
        districtsType.values.toArray.filter{
          case (did, tp) =>
            tp == type_id || tp == 0
        }.foreach{
          case (did,tp)=>
               Range(0,weatIdMap.size).foreach{
                 wid =>
                 gaps_map( (did,wid) ) = gaps_map.getOrElse((did,wid), Array[Double]()) ++ Array(fs.getOrElse((did,wid),0.0))
               }
        }

    }
    gaps_map.mapValues{
      fs =>
          val fs_vec = Vec(fs)
          ( fs_vec.mean,
            fs_vec.median,
            fs_vec.stdev,
            fs_vec.min.getOrElse(0.0),
            fs_vec.max.getOrElse(0.0))
        }
  }
  def run(feat_name:String ): Unit ={
  dates.foreach{
      date =>
        val feat_dir = ditech16.data_pt + s"/fs/$feat_name"
        Directory.create( feat_dir)
        val feat_fp = feat_dir + s"/${feat_name}_$date"

        val weather_data_fp = ditech16.data_pt + s"/weather_data/weather_data_$date"
        val weather_map = Weather.load_map(weather_data_fp)

        val feats = districts.values.toArray.sorted.flatMap { did =>
          Range(1, ditech16.max_new_time_id + 1).map {
            tid =>
              val weat = weather_map.getOrElse(tid, Weather.fillWeather(weather_map, tid ))
              val wid = weatIdMap( weat.weather )
              val f = stat_map.getOrElse((did, wid),(0,0,0,0,0))
              s"$did,$tid\t${f._1},${f._2},${f._3},${f._4},${f._5}"
          }
        }

        IO.write(feat_fp, feats )
    }
  }

}
