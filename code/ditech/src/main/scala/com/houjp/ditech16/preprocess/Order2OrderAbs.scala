package com.houjp.ditech16.preprocess

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{District, OrderAbs}

object Order2OrderAbs {

  def main(args: Array[String]) {
    // run(ditech16.train_pt)
    run(ditech16.test1_pt)
  }

  def run(data_pt: String): Unit = {
    val districts_fp = data_pt + "/cluster_map/cluster_map"
    val districts = District.load_local(districts_fp)

    val dates_fp = data_pt + "/dates"
    val dates = IO.load(dates_fp).distinct

    dates.foreach { date =>
      val input_fp = data_pt + s"/order_data/order_data_$date"
      val output_fp = data_pt + s"/order_abs_data/order_data_$date"

      transform(input_fp, output_fp, districts)
    }
  }

  def transform(input_fp: String, output_fp: String, districts: Map[String, Int]) = {

    val orders_abs: Array[OrderAbs] = OrderAbs.load_order_local(input_fp, districts)

    IO.write(output_fp, orders_abs.map(_.toString))
  }
}