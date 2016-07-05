package ditech.datastructure

import java.io.{File, PrintWriter}
import java.util.concurrent.{Executors, TimeUnit}

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{District, TimeSlice}
import ditech.common.util.Directory
import scopt.OptionParser

import scala.collection.mutable

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
  def run(fs_names:Array[String], train_pt:String, test_pt:String): Unit ={

    val train_offline_handler = new Handler(
      ditech16.train_pt + "/train_time_slices",
      train_pt + "/train_key",
      train_pt + "/train_libsvm",
      fs_names)

    val test_offline_handler = new Handler(
      ditech16.train_pt + "/test_time_slices",
      train_pt + "/test_key",
      train_pt + "/test_libsvm",
      fs_names)
    val val1_offline_handler = new Handler(
      ditech16.train_pt + "/val_time_slices1",
      train_pt + "/val_key1",
      train_pt + "/val_libsvm1",
      fs_names)
    val val2_offline_handler = new Handler(
        ditech16.train_pt + "/val_time_slices2",
      train_pt + "/val_key2",
      train_pt + "/val_libsvm2",
      fs_names)

    val train_online_handler = new Handler(
      ditech16.test1_pt + "/train_time_slices",
      test_pt + "/train_key",
      test_pt + "/train_libsvm",
      fs_names)
    val test_online_handler = new Handler(
      ditech16.test1_pt + "/test_time_slices",
      test_pt + "/test_key",
      test_pt + "/test_libsvm",
      fs_names)
    val val_online_handler = new Handler(
      ditech16.test1_pt + "/val_time_slices",
      test_pt + "/val_key",
      test_pt + "/val_libsvm",
      fs_names)

    threadPool.execute( train_offline_handler)
    threadPool.execute( train_online_handler)
    threadPool.execute( test_offline_handler)
    threadPool.execute( val1_offline_handler)
    threadPool.execute( val2_offline_handler)
    threadPool.execute( test_online_handler)
    threadPool.execute( val_online_handler)

    threadPool.shutdown()
   while( val_online_handler.feat_col_num == null ){
     threadPool.awaitTermination(10, TimeUnit.SECONDS)
   }
    var offset = 0
    val feature_indexs = val_online_handler.feat_col_num.map{
      case (fname, num)=>
        val start = offset
        offset = offset + num
       s"$fname\t$start-${start+num}"
    }.toArray
    feature_indexs.foreach( println )
    IO.write( train_pt + "/feature_index" , feature_indexs)
  }

  def optionParser():OptionParser[Params] = {
    val parser = new OptionParser[Params]("DataPoint"){
      opt[String]("fs_names")
        .text(s"the feature names' file path")
        .action( (x,c) => c.copy( features_name_pt = x))
      opt[String]("online_out")
        .text(s"the feature names' file path")
        .action( (x,c) => c.copy( online_out = x))
      opt[String]("offline_out")
        .text(s"the feature names' file path")
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
        run(fs_names, params.offline_out, params.online_out )
    }.getOrElse( System.exit(-1))

  }

  def load_label(date: String, fp: String, time_ids: Map[Int,Int]): Map[(Int, Int), Double] = {
    IO.load(fp).map { s =>
      val Array(key, gap_s) = s.split("\t")
      val Array(did_s, tid_s) = key.split(",")
      ( (did_s.toInt, tid_s.toInt), gap_s.toDouble)
    }.filter( e=> time_ids.contains( e._1._2 )).toMap
  }

 def loadFeatures(date:String,
                  fs_names:Array[String],
                  time_id:Map[Int,Int]):
 (Array[((Int, Int), Array[Double])], mutable.ArrayBuffer[(String, Int)]) ={

   val feat_idx = collection.mutable.ArrayBuffer[(String,Int)]()
    val fs_mix = fs_names.map {
      fs_name =>
        val fs_fp = ditech16.data_pt + s"/fs/$fs_name/${fs_name}_$date"
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
        feat_idx += ((fs_name, feat_num))
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

   (fs_mix.toArray,feat_idx)

  }

  class Handler(time_slice_fp: String,
          key_fp: String,
          libsvm_fp: String,
          fs_names: Array[String]) extends Runnable {

    var feat_col_num: collection.mutable.ArrayBuffer[(String, Int)] = null

    def run() {
      val time_slices = TimeSlice.load_local(time_slice_fp)
      val dates = time_slices.map(_.date).distinct
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
        val time_ids: Map[Int, Int] = time_slices.filter(_.date == date).map(x => (x.time_id, 1)).groupBy(_._1).mapValues(_.length)

        val label_fp = ditech16.data_pt + s"/label/label_$date"
        val labels = load_label(date, label_fp, time_ids)

        val (feats, feat_index) = loadFeatures(date, fs_names, time_ids)
        feat_col_num = feat_index
        feats.filter {
          case ((did, tid), feat) =>
            //如果在overview_dates中找不到，则为part2的test数据
            districtsType(did) == overview_dates.getOrElse(date, 2) || districtsType(did) == 0
        }.flatMap {
          case ((did, tid), feat) =>
            val rows = time_ids.getOrElse(tid, 0)
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


  case class Params(features_name_pt:String="features.conf", online_out:String = "", offline_out:String="")
}
