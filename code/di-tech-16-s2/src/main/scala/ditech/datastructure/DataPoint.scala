package ditech.datastructure

import java.io.{File, PrintWriter}
import java.util.concurrent.{Executors, TimeUnit}

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{District, TimeSlice}
import ditech.common.util.Directory
import scopt.OptionParser

import scala.collection.mutable.ArrayBuffer

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

  val threadPool = Executors.newFixedThreadPool(7)

  def run(fs_names:Array[String], time_slice_pt:String, train_pt:String, test_pt:String): Unit = {

    val handler_train_offline = new Handler(
      time_slice_pt + "/offline/train_time_slices",
      train_pt + "/train_key",
      train_pt + "/train_libsvm",
      fs_names)


    val handler_train_online = new Handler(
      time_slice_pt + "/online/train_time_slices",
      test_pt + "/train_key",
      test_pt + "/train_libsvm",
      fs_names)

    val handler_test_offline = new Handler(
      ditech16.offline_pt + "/test_time_slices",
      train_pt + "/test_key",
      train_pt + "/test_libsvm",
      fs_names)
    val handler_val1_offline = new Handler(
      ditech16.offline_pt + "/val_time_slices1",
      train_pt + "/val_key1",
      train_pt + "/val_libsvm1",
      fs_names)
    val handler_val2_offline = new Handler(
      ditech16.offline_pt + "/val_time_slices2",
      train_pt + "/val_key2",
      train_pt + "/val_libsvm2",
      fs_names)

    val handler_test_online = new Handler(
      ditech16.online_pt + "/test_time_slices",
      test_pt + "/test_key",
      test_pt + "/test_libsvm",
      fs_names)
    val handler_val_online = new Handler(
      ditech16.online_pt + "/val_time_slices",
      test_pt + "/val_key",
      test_pt + "/val_libsvm",
      fs_names)

    threadPool.execute(handler_train_offline)
    threadPool.execute(handler_train_online)
    threadPool.execute(handler_test_offline)
    threadPool.execute(handler_test_online)
    threadPool.execute(handler_val1_offline)
    threadPool.execute(handler_val2_offline)
    threadPool.execute(handler_val_online)

    threadPool.shutdown()

    while( true && !threadPool.awaitTermination(20, TimeUnit.SECONDS) ){
     println("Please waiting ...")
    }
    var offset = 0
    val feature_indexs = handler_val_online.feat_column_num.map {
      case (fname, num) =>
        val start = offset
        offset = offset + num
        s"$fname\t$start-${start + num}"
    }.toArray
    feature_indexs.foreach(println)
  }

  def optionParser():OptionParser[Params] = {
    val parser = new OptionParser[Params]("DataPoint"){
      opt[String]("fs_names")
        .text(s"the feature names' file path")
        .action( (x,c) => c.copy( features_name_pt = x))
      opt[String]("time_slice_pt")
        .text(s"the time slice file path")
        .action( (x,c) => c.copy( time_slice_pt = x))
      opt[String]("online_out")
        .text(s"the online libsvm output path")
        .action( (x,c) => c.copy( online_out = x))
      opt[String]("offline_out")
        .text(s"the offline libsvm output path")
        .action( (x,c) => c.copy( offline_out = x))
    }
    parser
  }

  def main(args: Array[String]) {

    val default_params = new Params()
    val parser = optionParser()
    parser.parse(args, default_params).map{
      params=>
       val fs_names = IO.load(params.features_name_pt).map( _.trim).filter{
         line =>
           !line.startsWith("#")
       }
        Directory.create( params.online_out)
        Directory.create( params.offline_out)
        run(fs_names, params.time_slice_pt, params.offline_out, params.online_out )
    }.getOrElse( System.exit(-1))

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

  def loadFeatures(date:String,
                   fs_names:Array[String],
                   time_id:Map[Int,Int]):
  (Array[((Int,Int), Array[Double])], ArrayBuffer[(String,Int)]) = {

    val feat_col_num = collection.mutable.ArrayBuffer[(String,Int)]()
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
        val feat_num: Int = fs.map( _._2.size ).reduce{
          (x,y) =>
            if( x == y)  x
            else -1
        }
        if( feat_num == -1 ){
          println( s"$fs_name features column doesn't identical")
          System.exit(-1)
        }
        feat_col_num += ((fs_name, feat_num))

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

    (fs_mix.toArray,feat_col_num)
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


  class Handler(new_time_slice_fp: String,
          key_fp: String,
          libsvm_fp: String,
          fs_names: Array[String]
  ) extends Runnable {

    var feat_column_num: collection.mutable.ArrayBuffer[(String, Int)] = null
    def run() {
      val new_time_slices = TimeSlice.load_old(new_time_slice_fp)
      val dates = new_time_slices.map(_.date).distinct
      val overview_dates = IO.load(ditech16.data_pt + "/overview_dates").map {
        line =>
          val Array(date_s, type_id) = line.split("\t")
          (date_s, type_id.toInt)
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

        val (feats, feat_col_num_tmp) = loadFeatures(date, fs_names, new_time_ids)
        feat_column_num = feat_col_num_tmp
        feats.filter {
          case ((did, tid), feat) =>
            //如果在overview_dates中找不到，则为part2的test数据
            districtsType(did) == overview_dates.getOrElse(date, 2) || districtsType(did) == 0
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
      feat_column_num
    }
  }

  case class Params(features_name_pt:String="features.conf", time_slice_pt:String ="", online_out:String = "", offline_out:String="")
}
