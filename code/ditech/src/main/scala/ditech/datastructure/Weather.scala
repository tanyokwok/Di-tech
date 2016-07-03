package ditech.datastructure

import com.houjp.common.io.IO

/**
  * Created by Administrator on 2016/6/6.
  */
class Weather(val time:String,
              val year:Int,
              val month:Int,
              val day:Int,
              val hour:Int,
              val minute:Int,
              val second:Int,
              val time_id:Int,
              val weather:Int,
              val temperature:Double,
              val PM25:Double) {

  override def toString:String = {
    s"weather($weather)," +
    s"temperature($temperature)," +
    s"PM25($PM25)," +
    s"year($year),month($month),day($day),hour($hour),minute($minute),second($second),time_id($time_id)"
  }
}

object Weather{

  def parse(s:String):Weather={
    val Array(t, weat, temp, pm ) = s.split("\t")
    val yy = t.substring(0,4).toInt
    val MM = t.substring(5,7).toInt
    val dd = t.substring(8,10).toInt
    val hh = t.substring(11,13).toInt
    val mm = t.substring(14,16).toInt
    val ss = t.substring(17).toInt
    val tid = (hh * 60 + mm) / 10 + 1

    new Weather(t,yy,MM,dd,hh,mm,ss,tid,weat.toInt,temp.toDouble,pm.toDouble)
  }

  def apply():Weather = {
    new Weather("",0,0,0,0,0,0,0,0,0.0,0.0)
  }
  def load_map(fp:String): Map[Int, Weather] ={
    IO.load(fp).map{ line =>
      val weat = parse(line)
      ( weat.time_id, weat)
    }.toMap
  }

   def load_weatherType(fp:String): Array[Int] ={

    IO.load(fp).map{ line =>
      val weat = parse(line)
      weat.weather
    }.distinct
  }
  def simWeather( weather_map:Map[Int,Weather], tid:Int ):Weather ={
    var k = 2
    var i = tid - 1
    var j = tid + 1
    while( i > 0 && j < 145 && k > 0  ){
      k = k - 1
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

    while( i > 0 && k > 0 ){
      k = k - 1
      if( weather_map.contains(i)) return weather_map(i)
      i = i - 1
    }
    while( j < 145 && k > 0 ){
      k = k - 1
      if( weather_map.contains(j)) return weather_map(j)
      j = j + 1
    }

    Weather()
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
