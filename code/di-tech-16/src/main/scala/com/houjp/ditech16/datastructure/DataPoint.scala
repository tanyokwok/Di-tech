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
    val fs_names = Array("pregap_1",
      "pregap_2",
      "pregap_3",
      "pregapave",
      "week",
      "tid")

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

  def load_local(date: String, fp: String, time_ids: Set[Int]): Array[DataPoint] = {

    IO.load(fp).map { s =>
      val Array(key, gap_s) = s.split("\t")
      val Array(did_s, tid_s) = key.split(",")
      ((did_s.toInt, tid_s.toInt), gap_s.toDouble)
    }.filter(e => time_ids.contains(e._1._2)).map { e =>
      val ts_s = s"$date-${e._1._2}"
      new DataPoint(e._1._1, TimeSlice.parse(ts_s), e._2, collection.mutable.ArrayBuffer[Double]())
    }
  }

  def append(date: String, fp: String, dps: Array[DataPoint], time_ids: Set[Int]) = {
    val new_fs = IO.load(fp).map { s =>
      val Array(key, fs_s) = s.split("\t")
      val Array(did_s, tid_s) = key.split(",")
      ((did_s.toInt, tid_s.toInt), fs_s.split(",").map(_.toDouble))
    }.filter(e => time_ids.contains(e._1._2))

    require(new_fs.length == dps.length, "n(DataPoint) != n(FeaturePoint)")

    new_fs.indices.foreach { id =>
      dps(id).fs ++= new_fs(id)._2
    }
  }

  def run(time_slice_fp: String,
          key_fp: String,
          libsvm_fp: String,
          fs_names: Array[String]): Unit = {

    val time_slices = TimeSlice.load_local(time_slice_fp)
    val dates = time_slices.map(_.date).distinct

    val all_data = collection.mutable.ArrayBuffer[DataPoint]()

    dates.foreach { date =>
      val time_ids: Set[Int] = time_slices.filter(_.date == date).map(_.time_id).toSet

      val label_fp = ditech16.s1_pt + s"/label/label_$date"
      val dps = load_local(date, label_fp, time_ids)

      fs_names.foreach { fs_name =>
        val fs_fp = ditech16.s1_pt + s"/fs/$fs_name/${fs_name}_$date"
        DataPoint.append(date, fs_fp, dps, time_ids)
      }

      all_data ++= dps
    }

    IO.write(key_fp, all_data.map(_.get_key()).toArray)
    IO.write(libsvm_fp, all_data.map(_.get_libsvm()).toArray)
  }
}