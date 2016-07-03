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
              val new_time_id: Int,
              val weather:Int,
              val temperature:Double,
              val PM25:Double) {

  override def toString:String = {
    s"weather($weather)," +
      s"temperature($temperature)," +
      s"PM25($PM25)," +
      s"year($year),month($month),day($day),hour($hour),minute($minute),second($second)," +
      s"time_id($time_id),new_time_id($new_time_id)"
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
    val new_tid = hh * 60 + mm + 1

    new Weather(t,yy,MM,dd,hh,mm,ss,tid, new_tid, weat.toInt,temp.toDouble,pm.toDouble)
  }

  def apply():Weather = {
    new Weather("",0,0,0,0,0,0,0,0,0,0.0,0.0)
  }
  def load_map(fp:String): Map[Int, Weather] ={
    IO.load(fp).map{ line =>
      val weat = parse(line)
      ( weat.new_time_id, weat)
    }.toMap
  }

   def load_weatherType(fp:String): Array[Int] ={

    IO.load(fp).map{ line =>
      val weat = parse(line)
      weat.weather
    }.distinct
  }
}
