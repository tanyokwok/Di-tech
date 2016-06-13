package ditech.feature

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District
import ditech.common.util.Directory
import ditech.datastructure.Weather

object FWeatherOHE {

  def main(args:Array[String]): Unit ={
    run(ditech16.s1_pt)
  }
  def run( data_pt:String ): Unit ={
     val districts_fp = data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)
    val date_fp = data_pt + "/dates"
    val dates = IO.load(date_fp).distinct

    val weat_map: Map[Int, Int] = dates.flatMap{
       date =>
        val weather_data_fp = data_pt + s"/weather_data/weather_data_$date"
        val weat_types= Weather.load_weatherType(weather_data_fp)
        weat_types
    }.distinct.sorted.zipWithIndex.toMap

    dates.foreach{
      date =>
        val weather_data_fp = data_pt + s"/weather_data/weather_data_$date"
        val weather_map = Weather.load_map(weather_data_fp)
        val weat_dir = data_pt + s"/fs/weatherOHE"
        Directory.create( weat_dir)
        val weat_fp = weat_dir + s"/weatherOHE_$date"

        val feats = districts.values.toArray.sorted.flatMap { did =>
          Range(1, 145).map {
            tid =>

              val weat = weather_map.getOrElse(tid, fillWeather(weather_map,tid))
              s"$did,$tid\t${oheWeat(weat.weather, weat_map)},${weat.temperature},${weat.PM25}"
          }
        }

        IO.write(weat_fp , feats )
    }
  }

  def oheWeat( weat:Int, weatMap:Map[Int,Int]): String={
    val vec = new Array[Int](weatMap.size)
    vec(weatMap(weat)) = 1
    val feat = new StringBuilder()
    vec.foreach{
      v =>
        feat.append(s"$v,")
    }
    feat.substring(0,feat.length - 1)
  }
  def fillWeather(weather_map:Map[Int,Weather],tid:Int): Weather={
    var i = tid - 1
    var j = tid + 1
    while( i > 0 && j < 145 ){
      if( weather_map.contains(i) && weather_map.contains(j) ){
        val weat_a = weather_map(i)
        val weat_b = weather_map(j)

        return new Weather( weat_a.time, weat_a.year, weat_a.month, weat_a.day, weat_a.hour, weat_a.minute, weat_a.second, weat_a.time_id,
          weat_a.weather, (weat_a.temperature + weat_b.temperature)/2, (weat_a.PM25 + weat_b.PM25)/2 )
      }else if( weather_map.contains(i) ){
        return weather_map(i)
      }else if( weather_map.contains(j) ){
        return weather_map(j)
      }
      i = i - 1
      j = j + 1
    }

    while( i > 0 ){
      if( weather_map.contains(i)) return weather_map(i)
      i = i - 1
    }
    while( j < 145 ){
      if( weather_map.contains(j)) return weather_map(j)
      j = j + 1
    }

    Weather()
  }
}
