package ditech.common.util

import java.text.SimpleDateFormat

/**
  * 选一个起始日期，提供方法使得对应日期每次增长一天
  */
class DateIncrement(val date_str:String){
  val simFormat = new SimpleDateFormat("yyyy-MM-dd")
  val date = simFormat.parse(date_str)

  def next(): Unit ={
    date.setDate( date.getDate + 1 )
  }

  override def toString = simFormat.format(date)
}

object DateIncrement {

  def apply(date_str:String): DateIncrement={
    return new DateIncrement(date_str)
  }

}
