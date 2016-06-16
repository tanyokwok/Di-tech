package ditech.feature

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District
import ditech.common.util.{DateIncrement, Directory}
import ditech.datastructure.TrafficAbs
import org.saddle.Vec

object FDTTrafficTotal {

  val districts_fp = ditech16.s1_pt + "/cluster_map/cluster_map"
  val districts = District.load_local(districts_fp)

  def main(args:Array[String]): Unit ={
    run(ditech16.s1_pt,this.getClass.getSimpleName.replace("$",""))
  }

  val stat_map = getStatisticsByDate("2016-01-01",21)
  def getStatisticsByDate(start_date:String, day_count:Int) ={
    val date = DateIncrement(start_date)
   //get gaps of every day
    val  gaps_map = Range(0,day_count).map{
      x=>
        val date_str = date.toString
        date.next()
        val traffic = TrafficAbs.load_local( ditech16.s1_pt + s"/traffic_data/traffic_data_$date_str",districts )

        val fs = collection.mutable.Map[(Int, Int), Double]()

        districts.values.toArray.sorted.foreach{
          did=>
            Range(1,145).foreach{ tid=>
              fs((did,tid)) = 0
            }
        }
        traffic.foreach { tfc =>
            fs((tfc.did, tfc.tid)) = tfc.level1 + tfc.level2 + tfc.level3 + tfc.level4
        }
        fs.mapValues( x => Array(x))
    }.reduce{
      (x,y) =>
        val z = ( x /: y){
          case (map, (k,v)) =>
           map + (k->( map(k) ++ v ))
        }
       z
    }

    gaps_map.mapValues{
      fs =>
        val fs_vec = Vec( fs )
        (fs_vec.mean, fs_vec.median, fs_vec.stdev,fs_vec.min.getOrElse(0.0), fs_vec.max.getOrElse(0.0))
//        (fs_vec.mean, fs_vec.median, fs_vec.stdev )
    }
  }
  def run( data_pt:String, feat_name:String ): Unit ={
   val date_fp = data_pt + "/dates"
   val dates = IO.load(date_fp).distinct
   dates.foreach{
      date =>
        val feat_dir = data_pt + s"/fs/$feat_name"
        Directory.create( feat_dir)
        val feat_fp = feat_dir + s"/${feat_name}_$date"

        val feats = districts.values.toArray.sorted.flatMap { did =>
          Range(1, 145).map {
            tid =>
              val f = stat_map.getOrElse((did,tid),(0,0,0,0,0))
              s"$did,$tid\t${f._1},${f._2},${f._3},${f._4},${f._5}"
//              val f = stat_map.getOrElse((did,tid),(0,0,0))
//              s"$did,$tid\t${f._1},${f._2},${f._3}"
          }
        }

        IO.write(feat_fp, feats )
    }
  }

}
