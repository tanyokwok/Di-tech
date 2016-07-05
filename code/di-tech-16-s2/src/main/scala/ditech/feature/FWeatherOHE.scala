package com.houjp.ditech16.feature

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District
import ditech.common.util.Directory
import ditech.datastructure.Weather

object FWeatherOHE {

  def main(args:Array[String]): Unit ={
    run(ditech16.data_pt, this.getClass.getSimpleName.replace("$",""))
  }

  def run( data_pt:String, f_name:String ): Unit ={
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

    val weat_dir = data_pt + s"/feature/$f_name"
    Directory.create( weat_dir)

    dates.foreach{
      date =>
        val weather_data_fp = data_pt + s"/weather_data/weather_data_$date"
        val weather_map = Weather.load_map(weather_data_fp)

        val weat_fp = weat_dir + s"/${f_name}_$date"

        val feats = districts.values.toArray.sorted.flatMap { did =>
          Range(1, 1440 + 1).map {
            ntid =>
              val weat = weather_map.getOrElse(ntid, Weather.fillWeather(weather_map,ntid))
              s"$did,$ntid\t${oheWeat(weat.weather, weat_map)},${weat.temperature},${weat.PM25}"
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

}
