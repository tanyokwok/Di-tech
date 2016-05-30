package com.houjp.ditech16.feature

import com.houjp.ditech16
import com.houjp.ditech16.datastructure.{Order, TimeSlice}
import org.scalatest.FunSuite

class PreGapTest extends FunSuite {

  val time_slices_fp = ditech16.test1_time_slices
  val time_slices = TimeSlice.load_local(time_slices_fp)

  val orders_fp = ditech16.test1_order_pt + "/order_data_test"
//  val orders = Order.load_local(orders_fp)
//
//  time_slices.zip(PreGap.run_local(orders, time_slices, 1)).foreach(println)
}