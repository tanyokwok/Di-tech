package ditech.datastructure

import java.io.{File, PrintWriter}

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.TimeSlice
import ditech.common.util.Directory
import scopt.OptionParser

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

  def run(fs_names:Array[String], train_pt:String, test_pt:String): Unit ={
    run(ditech16.train_pt + "/train_time_slices",
      train_pt + "/train_key",
      train_pt + "/train_libsvm",
      fs_names)
    run(ditech16.train_pt + "/test_time_slices",
      train_pt + "/test_key",
      train_pt + "/test_libsvm",
      fs_names)
    run(ditech16.train_pt + "/val_time_slices1",
      train_pt + "/val_key1",
      train_pt + "/val_libsvm1",
      fs_names)
      run(ditech16.train_pt + "/val_time_slices2",
      train_pt + "/val_key2",
      train_pt + "/val_libsvm2",
      fs_names)

    run(ditech16.test1_pt + "/train_time_slices",
      test_pt + "/train_key",
      test_pt + "/train_libsvm",
      fs_names)

    run(ditech16.test1_pt + "/test_time_slices",
      test_pt + "/test_key",
      test_pt + "/test_libsvm",
      fs_names)
    run(ditech16.test1_pt + "/val_time_slices",
      test_pt + "/val_key",
      test_pt + "/val_libsvm",
      fs_names)
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

 /*   val fs_names = Array(
      "week",
      "tid",
//      "tid",//"did",
//      "price_1", "price_2", "price_3",
      "poi",
//      "FComFineGap",
      "FComPreGap",
      "FComPreArrive",
//      "feat_mix",
//      "miniArrive","miniArriveSelf",
      "fineArriveSelf",
      "fineArrive",
      "finegap",
      "fineDemand",
//      "GDRate",
      "weatherOHE"
//      "FDTGap",
//      "FDTGapByHoliday",
//      "FDTDemand",
//      "FDTSupply",
//      "pregap_1","pregap_2","pregap_3","pregapave",
//      "demand_1","demand_2","demand_3",
//      "preArrive_1", "preArrive_2", "preArrive_3",
//      "preArriveSelf_1","preArriveSelf_2","preArriveSelf_3"
    )
//    "traffic_1", "traffic_2", "traffic_3")
//      "weather")
//      "preArrive66_1","preArrive66_2", "preArrive66_3")

    run(fs_names )
    */
  }

  def load_label(date: String, fp: String, time_ids: Map[Int,Int]): Map[(Int, Int), Double] = {
    IO.load(fp).map { s =>
      val Array(key, gap_s) = s.split("\t")
      val Array(did_s, tid_s) = key.split(",")
      ( (did_s.toInt, tid_s.toInt), gap_s.toDouble)
    }.filter( e=> time_ids.contains( e._1._2 )).toMap
  }

 def loadFeatures(date:String,fs_names:Array[String], time_id:Map[Int,Int]): Array[((Int,Int), Array[Double])] = {
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
  def run(time_slice_fp: String,
          key_fp: String,
          libsvm_fp: String,
          fs_names: Array[String]): Unit = {

    val time_slices = TimeSlice.load_local(time_slice_fp)
    val dates = time_slices.map(_.date).distinct
    //    val dates = time_slices.map(_.date)

    val all_data = collection.mutable.ArrayBuffer[DataPoint]()

    val key_writer = new PrintWriter(new File(key_fp))
    val libsvm_writer = new PrintWriter(new File(libsvm_fp))
    dates.foreach { date =>
      val time_ids: Map[Int, Int] = time_slices.filter(_.date == date).map(x => (x.time_id, 1)).groupBy(_._1).mapValues(_.length)

      val label_fp = ditech16.data_pt + s"/label/label_$date"
      val labels = load_label(date, label_fp, time_ids)

      val feats: Array[((Int, Int), Array[Double])] = loadFeatures(date, fs_names, time_ids)
      feats.flatMap {
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


  case class Params(features_name_pt:String="features.conf", online_out:String = "", offline_out:String="")
}
