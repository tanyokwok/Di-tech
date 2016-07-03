package ditech.feature

import com.houjp.common.io.IO
import com.houjp.ditech16
import ditech.common.util.Directory
import ditech.datastructure.OrderAbs
import ditech.feature.ftrait.FLastWeekTrait
import org.saddle.Vec

object FLastWeekGap extends FLastWeekTrait{
  def main(args:Array[String]): Unit ={
    run(ditech16.data_pt,this.getClass.getSimpleName.replace("$",""))
  }

  val stat_map = genFeatMap()
  def genFeatMap() ={

    val value_collection = collection.mutable.Map[(Int,Int),Array[Double]]()
   //get gaps of every day
    overview_dates.foreach{
     case (date_str, type_id)=>
        val orders = OrderAbs.load_local( ditech16.data_pt + s"/order_data/order_data_$date_str",districtIds )
        val fs = collection.mutable.Map[(Int,Int), Double]()

        orders.foreach { ord =>
          if (-1 != ord.start_district_id && !ord.has_driver) {
            fs((ord.start_district_id, ord.time_id) ) =
              fs.getOrElse((ord.start_district_id, ord.time_id),0.0) + 1.0
          }
        }
        districtTypes.filter{
          case (did, tp) =>
            tp == type_id || tp == 0
        }.foreach{
          case (did,tp)=>
            Range(1, ditech16.max_time_id + 1).foreach{
              tid =>
                value_collection( (did,tid) ) = value_collection.getOrElse( (did,tid), Array[Double]()) ++ Array(fs.getOrElse( (did,tid),0.0))
            }
        }

    }

    val part1_day_cnt = overview_dates.filter( _._2 == 1 ).size
    val part2_day_cnt = overview_dates.filter( _._2 == 2 ).size

    //将array补成一样的大小
    val new_collection: Map[(Int, Int), Array[Double]] = value_collection.map{
      case (key,value)=>
        val did = key._1
        if( districtTypes(did) == 1 ){
          (key, value.slice(1,8) ++ value ++ (new Array[Double](part2_day_cnt + 7*2) ))
        }else if(districtTypes(did) == 2){
          (key, (new Array[Double](part1_day_cnt - 7) ) ++ value.slice(1,8) ++
            value ++ value.slice( value.size - 7, value.size ) ++ value.slice( value.size - 7, value.size ) )
        }else{
          (key, value.slice(1,8) ++ value ++
            value.slice( value.size - 7, value.size ) ++ value.slice( value.size - 7, value.size ) )
        }
    }.toMap
    dates.zipWithIndex.map{
      case (date,index) =>
      val fs = new_collection.mapValues{
         value =>
          val arr = value.slice(index, index + 7)
           val fs_vec = Vec( arr )
           (fs_vec.mean, fs_vec.median, fs_vec.stdev,fs_vec.min.getOrElse(0.0), fs_vec.max.getOrElse(0.0))
       }
        (date, fs)
    }.toMap

  }
  def run( data_pt:String, feat_name:String ): Unit ={
   dates.foreach{
      date =>
        val feat_dir = data_pt + s"/fs/$feat_name"
        Directory.create( feat_dir)
        val feat_fp = feat_dir + s"/${feat_name}_$date"

        val fs_map = stat_map(date)
        val feats = districtIds.values.toArray.sorted.flatMap { did =>
          Range(1, 145).map {
            tid =>
              val f = fs_map.getOrElse( (did,tid),(0,0,0,0,0))
              s"$did,$tid\t${f._1},${f._2},${f._3},${f._4},${f._5}"
          }
        }

        IO.write(feat_fp, feats )
    }
  }

}
