package ditech.datastructure

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District


object POIRoot {
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
      val rootId = id.split("#")(0)
      if( !keyMap.contains(rootId)){
        keyMap += ( (rootId,keyMap.size))
      }
      ( keyMap(rootId), cnt.toInt)
    }.toMap
    (district_id,poi)
  }

  def load(poi_fp:String): Map[Int, POI] ={
    val pois = IO.load(poi_fp).map( parse)
    val districts_fp = ditech16.data_pt + "/cluster_map/cluster_map"
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

}
