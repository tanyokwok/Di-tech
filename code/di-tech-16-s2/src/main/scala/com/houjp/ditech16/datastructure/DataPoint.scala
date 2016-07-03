package com.houjp.ditech16.datastructure

import java.io.{File, PrintWriter}

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

    val fs_names = IO.load(ditech16.data_pt + "/features").filter(!_.contains("#"))

    run(args(0) + s"/${args(1)}_new_time_slices",
      args(0) + s"/${args(1)}_key",
      args(0) + s"/${args(1)}_libsvm",
      fs_names)

  }

  def load_label(date: String, fp: String, time_ids: Map[Int,Int]): Map[(Int, Int), Double] = {
    IO.load(fp).map { s =>
      val Array(key, gap_s) = s.split("\t")
      val Array(did_s, tid_s) = key.split(",")
      ( (did_s.toInt, tid_s.toInt), gap_s.toDouble)
    }.filter( e=> time_ids.contains( e._1._2 )).toMap
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
            new DataPoint(e._2, TimeSlice.parse_old(ts_s), e._3, collection.mutable.ArrayBuffer[Double]())
        }
    }
  }

  def loadFeatures(date:String,fs_names:Array[String], time_id:Map[Int,Int]): Array[((Int,Int), Array[Double])] = {
    val fs_mix = fs_names.map {
      fs_name =>
        val fs_fp = ditech16.data_pt + s"/feature/$fs_name/${fs_name}_$date"
        val fs: Map[(Int, Int), Array[Double]] = IO.load(fs_fp).map {
          line =>
            val Array(key, fs_s) = line.split("\t")
            val Array(did_s, tid_s) = key.split(",")
            (did_s.toInt, tid_s.toInt, fs_s)
        }.filter( e => time_id.contains( e._2 )).map{
          e =>
            ( (e._1, e._2), e._3.split(",").map(_.toDouble))
        }.toMap
        fs

    }.reduce {
      // merge map by key
      (x, y) =>
        val z = (x /: y) {
          case (map, (k, v)) =>
            map + (k -> (map(k) ++ v))
        }
        z
    }

    fs_mix.toArray
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

  def run(new_time_slice_fp: String,
          key_fp: String,
          libsvm_fp: String,
          fs_names: Array[String]): Unit = {

    val new_time_slices = TimeSlice.load_old(new_time_slice_fp)
    val dates = new_time_slices.map(_.date).distinct
    val overview_dates = IO.load(ditech16.data_pt + "/overview_dates").map{
      line =>
        val Array(date_s,type_id) = line.split("\t")
        (date_s,type_id.toInt)
    }.toMap

    val all_data = collection.mutable.ArrayBuffer[DataPoint]()

    val key_writer = new PrintWriter(new File(key_fp))
    val libsvm_writer = new PrintWriter(new File(libsvm_fp))
    val districts_fp = ditech16.data_pt + "/cluster_map/cluster_map"
    val districtsType = District.loadDidTypeId(districts_fp).values.toMap

    dates.foreach { date =>
      val new_time_ids: Map[Int, Int] = new_time_slices.filter(_.date == date).map(x => (x.time_id, 1)).groupBy(_._1).mapValues(_.length)

      val label_fp = ditech16.data_pt + s"/label/label_$date"
      val labels = load_label(date, label_fp, new_time_ids)

      val feats: Array[((Int, Int), Array[Double])] = loadFeatures(date, fs_names, new_time_ids)
      feats.filter{
        case ((did,tid),feat) =>
          //如果在overview_dates中找不到，则为part2的test数据
          districtsType(did) == overview_dates.getOrElse( date, 2 ) || districtsType(did) == 0
      }.flatMap {
        case ((did, tid), feat) =>
          val rows = new_time_ids.getOrElse(tid, 0)
          Range(0, rows).map { x => (did, tid, feat) }
      }.foreach {
        case (did, tid, feat) =>
          val ts_s = s"$date-${tid}"
          key_writer.write(s"${did},$ts_s\n")
          val gap = labels.getOrElse((did, tid), 0)
          val fs_s = s"$gap ${feat.zipWithIndex.map(x => s"${x._2 + 1}:${x._1}").mkString(" ")}"
          libsvm_writer.write(fs_s + "\n")
      }

    }
    key_writer.close()
    libsvm_writer.close()
  }
}
