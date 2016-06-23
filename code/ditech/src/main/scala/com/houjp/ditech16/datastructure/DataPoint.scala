package com.houjp.ditech16.datastructure

import com.houjp.common.io.IO
import com.houjp.ditech16

class DataPoint(val district_id: Int,
                val time_slice: TimeSlice,
                val gap: Double,
                val fs: collection.mutable.ArrayBuffer[Double]) {

  def get_key(): String = {
    s"$district_id,$time_slice"
  }

  def get_libsvm(): String = {
    s"$gap ${fs.zipWithIndex.map(e => s"${e._2 + 1}:${e._1}").mkString(" ")}"
  }

  override def toString: String = {
    s"$district_id,$time_slice\t$gap\t${fs.mkString(",")}"
  }
}

object DataPoint {

  def main(args: Array[String]) {
    val fs_names = Array(
      "week",
      "tid",
//      "tid",//"did",
//      "price_1", "price_2", "price_3",
      "poi",
//      "miniArrive","miniArriveSelf",
//      "fineArriveSelf",
//      "fineArrive_1","fineArrive_2","fineArrive_3","fineArrive_4","fineArrive_5",
//      "finegap_1","finegap_2","finegap_3","finegap_4","finegap_5",
      "GDRate",
      "weatherOHE",
      "pregap_1","pregap_2","pregap_3","pregapave",
      "demand_1","demand_2","demand_3",
      "preArrive_1", "preArrive_2", "preArrive_3",
      "preArriveSelf_1","preArriveSelf_2","preArriveSelf_3")
//    "traffic_1", "traffic_2", "traffic_3")
//      "weather")
//      "preArrive66_1","preArrive66_2", "preArrive66_3")


    run(ditech16.train_pt + "/train_time_slices",
      ditech16.train_pt + "/train_key",
      ditech16.train_pt + "/train_libsvm",
      fs_names)
    run(ditech16.train_pt + "/test_time_slices",
      ditech16.train_pt + "/test_key",
      ditech16.train_pt + "/test_libsvm",
      fs_names)

    run(ditech16.test1_pt + "/train_time_slices",
      ditech16.test1_pt + "/train_key",
      ditech16.test1_pt + "/train_libsvm",
      fs_names)
    run(ditech16.test1_pt + "/test_time_slices",
      ditech16.test1_pt + "/test_key",
      ditech16.test1_pt + "/test_libsvm",
      fs_names)

  }

  def load_local(date: String, fp: String, time_ids: Array[Int]): Array[DataPoint] = {

    val time_id_set = time_ids.toSet
    val records_map: Map[Int, Array[(Int, Int, Double)]] = IO.load(fp).map { s =>
      val Array(key, gap_s) = s.split("\t")
      val Array(did_s, tid_s) = key.split(",")
      (tid_s.toInt, did_s.toInt, gap_s.toDouble)
    }.filter( e => time_id_set.contains(e._1)).groupBy(_._1)

    time_ids.flatMap{
      tid =>
        val records: Array[(Int, Int, Double)] = records_map.getOrElse(tid,Array[(Int,Int,Double)]())

        records.map {
          e =>
            val ts_s = s"$date-${e._1}"
            new DataPoint(e._2, TimeSlice.parse(ts_s), e._3, collection.mutable.ArrayBuffer[Double]())
        }
    }
  }

  def append(date: String, fp: String, dps: Array[DataPoint], time_ids: Array[Int]) = {
    val time_id_set = time_ids.toSet
    val new_fs: Map[(Int, Int), Array[Double]] = IO.load(fp).map { s =>
      val Array(key, fs_s) = s.split("\t")
      val Array(did_s, tid_s) = key.split(",")
      ((did_s.toInt, tid_s.toInt), fs_s.split(",").map(_.toDouble))
    }.filter(e => time_id_set.contains(e._1._2)).toMap

//    require(new_fs.length == dps.length, "n(DataPoint) != n(FeaturePoint)")

    dps.foreach{
      dp =>
        val fs = new_fs( (dp.district_id,dp.time_slice.time_id) )
        dp.fs ++= fs
    }
  }

  def run(time_slice_fp: String,
          key_fp: String,
          libsvm_fp: String,
          fs_names: Array[String]): Unit = {

    val time_slices = TimeSlice.load_local(time_slice_fp)
    val dates = time_slices.map(_.date).distinct
//    val dates = time_slices.map(_.date)

    val all_data = collection.mutable.ArrayBuffer[DataPoint]()

    dates.foreach { date =>
      val time_ids: Array[Int] = time_slices.filter(_.date == date).map(_.time_id)

      val label_fp = ditech16.data_pt + s"/label/label_$date"
      val dps = load_local(date, label_fp, time_ids)

      fs_names.foreach { fs_name =>
        val fs_fp = ditech16.data_pt + s"/fs/$fs_name/${fs_name}_$date"
        DataPoint.append(date, fs_fp, dps, time_ids)
      }

      all_data ++= dps
    }

    IO.write(key_fp, all_data.map(_.get_key()).toArray)
    IO.write(libsvm_fp, all_data.map(_.get_libsvm()).toArray)
  }
}
