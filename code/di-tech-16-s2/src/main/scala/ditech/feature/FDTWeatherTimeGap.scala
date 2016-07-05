package ditech.feature

import java.text.SimpleDateFormat
import java.util.Calendar

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{District, OrderAbs}
import ditech.common.util.Directory
import ditech.datastructure.Weather
import org.saddle.Vec

object FDTWeatherTimeGap {

  val districts_fp = ditech16.data_pt + "/cluster_map/cluster_map"
  val districtsType = District.loadDidTypeId(districts_fp)
  val date_fp = ditech16.data_pt + "/dates"
  val dates = IO.load(date_fp).distinct
  val districts = districtsType.mapValues(_._1)
  val weatIdMap: Map[Int, Int] = dates.flatMap {
    date =>
      val weather_data_fp = ditech16.data_pt + s"/weather_data/weather_data_$date"
      val weat_types = Weather.load_weatherType(weather_data_fp)
      weat_types
  }.distinct.sorted.zipWithIndex.toMap

  def main(args: Array[String]): Unit = {
    run(this.getClass.getSimpleName.replace("$", ""))
  }

  def increase(fs: collection.mutable.Map[(Int, Int), Double],
               ntid: Int, wid:Int) {
   fs((ntid, wid)) =
      fs.getOrElse((ntid, wid), 0.0) + 1.0
  }

  class Handler(now_date:String) extends Runnable{
    val fs = collection.mutable.Map[(Int, Int), Double]()
    def run(): Unit ={

       val date_formate = new SimpleDateFormat("yyyy-MM-dd")
       val cal = Calendar.getInstance()
       cal.setTime(date_formate.parse(now_date))
       cal.set(Calendar.DATE, cal.get(Calendar.DATE) + 1)
       val aft_date = date_formate.format(cal.getTime)

       val aft_orders = OrderAbs.load_local(ditech16.data_pt + s"/order_abs_data/order_data_$aft_date")
       val aft_weather_data_fp = ditech16.data_pt + s"/weather_data/weather_data_$now_date"
       val aft_weather_map = Weather.load_map(aft_weather_data_fp)

       val now_orders = OrderAbs.load_local( ditech16.data_pt + s"/order_abs_data/order_data_$now_date")
       val now_weather_data_fp = ditech16.data_pt + s"/weather_data/weather_data_$now_date"
       val now_weather_map = Weather.load_map(now_weather_data_fp)

       aft_orders.foreach{
         ord =>
           val s_ntid = ord.new_time_id - 9
           val e_ntid = ord.new_time_id - 0

           if (-1 != ord.start_district_id && !ord.has_driver) {
             Range(s_ntid + ditech16.max_new_time_id, ditech16.max_new_time_id + 1).foreach {
               ntid =>
                 val wtid = ntid + 5

                val wid = if( wtid > ditech16.max_new_time_id ){
                   val weat = aft_weather_map.getOrElse(ntid,
                     Weather.fillWeather(aft_weather_map, wtid - ditech16.max_new_time_id))
                   weatIdMap(weat.weather)
                 }else{
                   val weat = now_weather_map.getOrElse(ntid,
                     Weather.fillWeather(now_weather_map, wtid))
                   weatIdMap(weat.weather)
                }
                 increase( fs, ntid, wid)
             }
           }
       }

       now_orders.foreach {
         ord =>
           val s_ntid = ord.new_time_id - 9
           val e_ntid = ord.new_time_id - 0

           if (-1 != ord.start_district_id && !ord.has_driver) {
             Range(math.max(1, s_ntid), e_ntid + 1).foreach {
               ntid =>
                  val wtid = ntid + 5

                val wid = if( wtid > ditech16.max_new_time_id ){
                   val weat = aft_weather_map.getOrElse(ntid,
                     Weather.fillWeather(aft_weather_map, wtid - ditech16.max_new_time_id))
                   weatIdMap(weat.weather)
                 }else{
                   val weat = now_weather_map.getOrElse(ntid,
                     Weather.fillWeather(now_weather_map, wtid))
                   weatIdMap(weat.weather)
                }
                 increase( fs, ntid, wid)
             }
           }
       }

    }
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
    val handlers = dates_arr.map{
      case (now_date, type_id) =>
        val handler = new Handler(now_date)
        handler.run()
        handler
    }

    handlers.foreach {
      handler =>
       Range(1,ditech16.max_new_time_id + 1).foreach{
          tid =>
               Range(0,weatIdMap.size).foreach{
                 wid =>
                 gaps_map( (tid,wid) ) = gaps_map.getOrElse((tid,wid), Array[Double]()) ++
                   Array(handler.fs.getOrElse((tid,wid),0.0))
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
              val f = stat_map.getOrElse((tid, wid),(0,0,0,0,0))
              s"$did,$tid\t${f._1},${f._2},${f._3},${f._4},${f._5}"
          }
        }

        IO.write(feat_fp, feats )
    }
  }

}
