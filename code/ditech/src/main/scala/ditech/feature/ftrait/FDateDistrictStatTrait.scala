package ditech.feature.ftrait

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{ District}
import ditech.common.util.Directory
import ditech.datastructure.OrderAbs
import org.saddle.Vec

import scala.collection.mutable

/**
  * Created by Administrator on 2016/6/27.
  */
trait FDateDistrictStatTrait {

  val districts_fp = ditech16.data_pt + "/cluster_map/cluster_map"
  val districts = District.loadDidTypeId(districts_fp)
  val districtIds = districts.mapValues( _._1 )
   val dates = IO.load(ditech16.data_pt + "/dates").distinct
  val overview_dates = IO.load(ditech16.data_pt + "/overview_dates").map{
      line =>
        val Array(date,type_s) = line.split("\t")
        (date, type_s.toInt)
    }

  def collect_order(ord:OrderAbs,fs:collection.mutable.Map[(Int,Int), Double])

  def genTensor(): mutable.Map[(Int, String), Array[Double]] ={

    val did_dates_tid_tensor = collection.mutable.Map[(Int,String), Array[Double]]()
   //get gaps of every day
    overview_dates.foreach{
     case (date_str, type_id)=>
        val orders = OrderAbs.load_local( ditech16.data_pt + s"/order_data/order_data_$date_str",districtIds )
        val fs = collection.mutable.Map[(Int,Int), Double]()

        orders.foreach( collect_order(_, fs))

        districts.values.toArray.filter{
          case (did, tp) =>
            tp == type_id || tp == 0
        }.foreach{
          case (did,tp)=>
            Range(1,ditech16.max_time_id + 1 ).foreach{
              tid =>
                did_dates_tid_tensor( (did,date_str) ) = did_dates_tid_tensor.getOrElse((did,date_str), Array[Double]()) ++
                  Array(fs.getOrElse((did,tid),0.0))
            }
        }

    }

    did_dates_tid_tensor
  }
 def genFeatMap() ={

    val did_dates_tid_tensor = genTensor()

    val did_dates_mat: collection.Map[(Int, String), (Double, Double, Double, Double, Double)] =
      did_dates_tid_tensor.mapValues{
      fs =>
        val fs_vec = Vec( fs )
        (fs_vec.mean, fs_vec.median, fs_vec.stdev,fs_vec.min.getOrElse(0.0), fs_vec.max.getOrElse(0.0))
    }

    did_dates_mat.groupBy( _._1._1).map{//按照did聚合
      group =>
        val did = group._1

        val grp: collection.Map[(Int, String), (Double, Double, Double, Double, Double)] = group._2
        val stats_arr = grp.map{//每天的5个统计值，合并成5个数组
          case (key, value) =>
            (Array(value._1),Array(value._2),Array(value._3),Array(value._4),Array(value._5))
        }.reduce{
          (x,y)=>
            (x._1 ++ y._1,
              x._2 ++ y._2,
              x._3 ++ y._3,
              x._4 ++ y._4,
              x._5 ++ y._5)
        }

       //统计5个数组的统计值
        val arrs = Array( stats_arr._1, stats_arr._2, stats_arr._3, stats_arr._4, stats_arr._5)
        val feats = arrs.map {
          arr =>
            val fs_vec = Vec(arr)
            val feat = Array(fs_vec.mean, fs_vec.median, fs_vec.stdev, fs_vec.min.getOrElse(0.0), fs_vec.max.getOrElse(0.0))
            feat
        }.reduce( _ ++ _ )
        (did,feats)
    }.toMap
  }


  val stat_map: Map[Int, Array[Double]] = genFeatMap()

  def run( feat_name:String ): Unit ={
    dates.foreach{
      date =>
        val feat_dir = ditech16.data_pt + s"/fs/$feat_name"
        Directory.create( feat_dir)
        val feat_fp = feat_dir + s"/${feat_name}_$date"

        val feats = districtIds.values.toArray.sorted.flatMap { did =>
          val f = stat_map.getOrElse(did, new Array[Double](25))
          Range(1, 145).map {
            tid =>
              val feat_s = new StringBuilder(s"$did,$tid\t")
              f.foreach{
                fs=>
                  feat_s.append( s"$fs,")
              }
              feat_s.substring(0,feat_s.length - 1)
          }
        }

        IO.write(feat_fp, feats )
    }
  }

}
