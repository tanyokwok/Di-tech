package ditech.feature

import com.houjp.common.io.IO
import com.houjp.ditech16
import ditech.common.util.Directory

/**
  * Feature combine pregaps
  */
object FComFineDemand {

  def main(args:Array[String]): Unit ={
    val fs_names = Array( FineDemand.getClass.getSimpleName.replace("$",""))
    run(fs_names,FComFineDemand.getClass.getSimpleName.replace("$",""))
  }

  def run(fs_names:Array[String],new_fs_name:String): Unit ={

    val date_fp = ditech16.data_pt + "/dates"
    val dates = IO.load(date_fp).distinct

    // load all features
    dates.foreach {
      date =>

        val feats_array: Array[(String, Array[Double])] = loadFeatures(date, fs_names)

        val new_fs = feats_array.map {
          case (key, feats) =>
            val new_fs = Range(0, feats.length).flatMap { i =>
              Range(i, feats.length).flatMap { j =>
                if (i != j) {
                  transform(feats(i), feats(j))
                } else {
                  Array[Double]()
                }
              }
            }

            val feat = new StringBuilder(key + "\t")
            new_fs.foreach {
              v =>
                feat.append(s"$v,")
            }
            feat.substring(0, feat.length - 1)
        }

        Directory.create(ditech16.data_pt + s"/fs/$new_fs_name")
        val fs_mix_fp = ditech16.data_pt + s"/fs/$new_fs_name/${new_fs_name}_$date"
        IO.write(fs_mix_fp, new_fs)
    }
  }

  def transform(x:Double,y:Double): Array[Double] ={
    Array( y - x,
      x / (if( math.abs( y ) < Double.MinPositiveValue) 1e-6 else y ),
      y / (if( math.abs( x ) < Double.MinPositiveValue) 1e-6 else x )
      )
  }
  /**load features from files listed in fs_names */
  def loadFeatures(date:String,fs_names:Array[String]): Array[(String, Array[Double])] = {
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
