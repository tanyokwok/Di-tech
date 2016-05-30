package com.houjp.ditech16.datastructure

import org.scalatest.FunSuite
import com.houjp.ditech16

class DataPointTest extends FunSuite {

  /*

  // test DataPoint.load_local & DataPoint.append

  val label_fp = "/Users/hugh_627/ICT/competition/di-tech-16/data/raw/season_1/label/label_2016-01-14"
  val ts_fp = "/Users/hugh_627/ICT/competition/di-tech-16/data/raw/season_1/training_data/train_time_slices"
  val date = "2016-01-14"

  val time_slices = TimeSlice.load_local(ts_fp)
  val time_ids: Set[Int] = time_slices.filter(_.date == date).map(_.time_id).toSet

  val dps = DataPoint.load_local(date, label_fp, time_ids)

  val pregap_1_fp = ditech16.s1_pt + s"/fs/pregap_1_$date"
  DataPoint.append(date, pregap_1_fp, dps, time_ids)

  val pregap_2_fp = ditech16.s1_pt + s"/fs/pregap_2_$date"
  DataPoint.append(date, pregap_2_fp, dps, time_ids)

  val pregap_3_fp = ditech16.s1_pt + s"/fs/pregap_3_$date"
  DataPoint.append(date, pregap_3_fp, dps, time_ids)

  val pregapave_fp = ditech16.s1_pt + s"/fs/pregapave_$date"
  DataPoint.append(date, pregapave_fp, dps, time_ids)

  dps.foreach(println)

  */

  // DataPoint.run(ditech16.train_pt, Array("pregap_1", "pregap_2", "pregap_3", "pregapave"))

}