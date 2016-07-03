package ditech.preprocess

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District

/**
  * Created by Administrator on 2016/6/23.
  */
class SampleKey {

  def main(args:Array[String]): Unit ={
    val dates_part1 = IO.load( ditech16.sample_key_pt + "/dates_part1")
    val dates_part2 = IO.load( ditech16.sample_key_pt + "/dates_part2")

    val district_map = District.loadDidTypeId(ditech16.cluster_pt + "/cluster_map")
    dates_part1.foreach{
      date =>
        //过滤掉只在part2中出现的地区
       val keys = district_map.values.toArray.sorted.filter( _._2 != 2 )
       .map{
         case (did,tp)=>
           s"$date\t$did"
       }
        IO.write(ditech16.sample_key_pt + s"/sample_key_$date", keys)
    }
    dates_part2.foreach{
      date =>
        //过滤掉只在part1中出现的地区
       val keys = district_map.values.toArray.sorted.filter( _._2 != 1 )
       .map{
         case (did,tp)=>
           s"$date\t$did"
       }
        IO.write(ditech16.sample_key_pt + s"/sample_key_$date", keys)
    }
  }
}
