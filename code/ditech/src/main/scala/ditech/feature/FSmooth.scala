package ditech.feature

import com.houjp.common.io.IO
import com.houjp.ditech16
import ditech.common.util.Directory

/**
  * Created by lvfuyu on 16/6/27.
  */
object FSmooth {

  def run(fs_names:Array[String], new_fs_name:String): Unit ={
    val date_fp = ditech16.data_pt + "/dates"
    val dates = IO.load(date_fp).distinct

    // load Original feature
    dates.foreach{
      date =>
        // current date -> all features: [did, tid, features]
        val feats_array: Array[(String, Array[Double])] = loadFeatures(date, fs_names)

        val new_fs = feats_array.map{
          case(key, feats) =>
            val smooth_feats = feats.map{
              i =>
                math.log(1+i) // smooth
            }

            val new_feats = new StringBuilder(key + "\t")
            smooth_feats.foreach{
              v =>
                new_feats.append(s"$v,")
            }
            new_feats.substring(0, new_feats.length-1)
        }

        Directory.create(ditech16.data_pt + s"/fs/$new_fs_name")
        val fs_smooth_fp = ditech16.data_pt + s"/fs/$new_fs_name/${new_fs_name}_$date"
        IO.write(fs_smooth_fp, new_fs)

    }
  }

  def loadFeatures(date:String, fs_names:Array[String]): Array[(String, Array[Double])] = {

    val fs_mix = fs_names.map {
      fs_name =>
        val fs_fp = ditech16.data_pt + s"/fs/$fs_name/${fs_name}_$date"
        val fs: Map[String, Array[Double]] = IO.load(fs_fp).map {
          line =>
            val Array(key, fs_s) = line.split("\t")
            (key, fs_s.split(",").map(_.toDouble))
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
}
