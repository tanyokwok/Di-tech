package ditech.feature

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District
import ditech.common.util.Directory
import ditech.datastructure.Weather

object FWeather {

  def main(args:Array[String]): Unit ={
    run(ditech16.data_pt,FWeather.getClass.getSimpleName.replace("$",""))
  }
  def run( data_pt:String, f_name:String ): Unit ={
     val districts_fp = data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)
    val date_fp = data_pt + "/dates"
    val dates = IO.load(date_fp).distinct

    dates.foreach{
      date =>
        val weather_data_fp = data_pt + s"/weather_data/weather_data_$date"
        val weather_map = Weather.load_map(weather_data_fp)
        val weat_dir = data_pt + s"/fs/${f_name}"
        Directory.create( weat_dir)
        val weat_fp = weat_dir + s"/${f_name}_date"

        val feats = districts.values.toArray.sorted.flatMap { did =>
          Range(1, 145).map {
            tid =>
              val weat = weather_map.getOrElse(tid, Weather.simWeather(weather_map,tid))
              s"$did,$tid\t${weat.weather},${weat.temperature},${weat.PM25}"
          }
        }

        IO.write(weat_fp , feats )
    }
  }

}
