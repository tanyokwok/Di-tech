package ditech.feature.ftrait

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.District

/**
  * Created by Administrator on 2016/6/26.
  */
trait FLastWeekTrait {

  val districts_fp = ditech16.data_pt + "/cluster_map/cluster_map"
  val districts = District.loadDidTypeId(districts_fp)
  val districtIds = districts.mapValues( _._1 )
  val districtTypes = districts.values.toMap

  val overview_dates = IO.load(ditech16.data_pt + "/overview_dates").map{
      line =>
        val Array(date,type_s) = line.split("\t")
        (date, type_s.toInt)
    }.sortBy(_._1)

  val dates = IO.load(ditech16.data_pt + "/dates").distinct.sorted
}
