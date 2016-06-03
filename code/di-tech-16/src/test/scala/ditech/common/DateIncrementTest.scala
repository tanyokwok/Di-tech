package ditech.common

import ditech.common.util.DateIncrement
import org.scalatest.FunSuite

/**
  * Created by Administrator on 2016/5/27.
  */
class DateIncrementTest extends FunSuite{
    val date = DateIncrement("2016-01-01")
    Range(0,100).foreach{
      x =>
      println( date )
      date.next()
    }

}
