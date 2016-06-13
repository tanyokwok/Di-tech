package ditech.datastructure

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District

class POI(val district_id:String,
          val poi: Array[Int]){

  override def toString ={
    val builder = new StringBuilder("")
    poi.foreach{
      x =>
      builder.append(s"$x,")
    }
    builder.substring(0,builder.length- 1)
  }
}
object POI {
  def apply(): POI={
    val vec = new Array[Int](keyMap.size)
    new POI("",vec)
  }
  val keyMap = collection.mutable.Map[String,Int]()
  def parse(line:String):(String,Map[Int,Int])={
    val arr = line.split("\t")
    val district_id = arr(0)
    val poi = Range(1,arr.length).map{ i =>
      val Array(id,cnt) = arr(i).split(":")
      if( !keyMap.contains(id)){
        keyMap += ( (id,keyMap.size))
      }
      ( keyMap(id), cnt.toInt)
    }.toMap
    (district_id,poi)
  }

  def load(poi_fp:String): Map[Int, POI] ={
    val pois = IO.load(poi_fp).map( parse)
    val districts_fp = ditech16.s1_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

    pois.map{
      case (did,poi) =>
       val vec = new Array[Int](keyMap.size)
        poi.foreach{
          case (id,cnt)=>
            vec(id) = cnt
        }

        (districts(did),new POI(did,vec))
    }.toMap

  }

  def dump(poi_fp:String,poiwec_fp:String): Unit ={
    val poi_map = load(poi_fp)
    val poi_vecs = poi_map.keySet.toArray.sorted.map{
      id =>
        val poi = poi_map(id)
        val vec = new StringBuilder("")
        poi.poi.foreach{
          v =>
            vec.append(s"$v ")
        }
        println(s"$id ${vec.toString}")
        vec.toString
    }

    IO.write(poiwec_fp,poi_vecs)
  }

  def loadKmeans(): Map[Int,Int]={
    IO.load(ditech16.s1_pt + "/poi_data/poi_kmeans").map{
      line =>
        val Array(id,cls) = line.trim.split("\\s+")
//        println( s"$id $cls")
        (id.toInt,cls.toInt)
    }.toMap
  }

  def main(args:Array[String]): Unit ={

    dump(ditech16.s1_pt + "/poi_data/poi_data", ditech16.s1_pt + "/poi_data/poi_vecs")
//    loadKmeans()
  }


}
