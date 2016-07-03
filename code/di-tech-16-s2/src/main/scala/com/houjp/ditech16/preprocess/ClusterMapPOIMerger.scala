package com.houjp.ditech16.preprocess

import com.houjp.common.io.IO
import com.houjp.ditech16

/**
  * Created by Administrator on 2016/6/23.
  */
object ClusterMapPOIMerger {


  def mergeCluster(): Unit = {

    val map = IO.load(ditech16.cluster_pt + "/cluster_map_s2").map {
      line =>
        val Array(district_hash, did) = line.split("\t")
        (district_hash, (did.toInt, 2) )
    }.toMap

    val cluster_map = collection.mutable.Map( map.toSeq: _*)
    IO.load( ditech16.cluster_pt + "/cluster_map_s1").foreach{
      line =>
        val Array(district_hash, did) = line.split( "\t")
        if( cluster_map.contains( district_hash)){
          val (did, tp ) = cluster_map(district_hash)
          cluster_map(district_hash) = ( did, 0 )
        }
        else
          cluster_map += (( district_hash , (cluster_map.size + 1, 1)) )
    }
    val cluster_strs = cluster_map.toArray.sortBy( _._2._1).map{
      case ( dhash, (did, tp))=>
        s"$dhash\t$did\t$tp"
    }

    IO.write(ditech16.cluster_pt + "/cluster_map", cluster_strs)
  }

  def mergePOI(): Unit ={
    val poi_strs = IO.load( ditech16.poi_pt + "/poi_data_s2") ++
      IO.load( ditech16.poi_pt + "/poi_data_s1")

    IO.write( ditech16.poi_pt + "/poi_data", poi_strs.distinct)
  }
  def main(args:Array[String]) {

    mergeCluster()
    mergePOI()
  }
}
