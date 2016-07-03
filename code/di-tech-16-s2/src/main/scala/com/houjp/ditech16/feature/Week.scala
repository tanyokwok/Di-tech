package com.houjp.ditech16.feature

import java.text.SimpleDateFormat
import java.util.Calendar

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District
import ditech.common.util.Directory

object Week {

  def main(args: Array[String]) {
    run(ditech16.data_pt, this.getClass.getSimpleName.replace("$",""))
  }

  def run(data_pt: String, f_name:String): Unit = {
    val districts_fp = data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

    val date_fp = data_pt + "/dates"
    val dates = IO.load(date_fp).distinct

    val sdf = new SimpleDateFormat("yyyy-MM-dd");
    val cld = Calendar.getInstance()

    dates.foreach { date =>
      val week = collection.mutable.Map[(Int, Int), Double]()
      val week_dir = data_pt + s"/feature/$f_name"
      Directory.create(week_dir)
      val week_fp = week_dir + s"/${f_name}_$date"

      cld.setTime(sdf.parse(date))

      val week_s = districts.values.toArray.sorted.flatMap { did =>
        Range(1, 1440 + 1).map { ntid =>
          val v = Array.fill(8)(0.0)

          val wid = cld.get(Calendar.DAY_OF_WEEK)
          v(wid - 1) = 1.0
          if (wid == 1 || wid == 7 || "2016-01-01".equals(date)) {
            v(7) = 1.0
          }
          s"$did,$ntid\t${v.mkString(",")}"
        }
      }
      IO.write(week_fp, week_s)
    }
  }
}