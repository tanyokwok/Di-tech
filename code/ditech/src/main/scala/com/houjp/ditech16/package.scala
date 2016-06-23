package com.houjp

package object ditech16 {

  val is_local = true

  // HDFS
  // val project_pt = "/user/houjp/di-tech-16/"

  // Mac

  // Windows
//  val project_pt = "/home/gty/ditech/"
  val project_pt = "../"
//  val project_pt = "E:/Di-tech/"
//  val data_pt = project_pt + "/data/"
  val data_pt = project_pt + "/data/raw/"
  val ans_pt = project_pt + "/data/ans/"

  val train_pt = data_pt + "/training_set/"
  val train_cluater_pt = train_pt + "/cluster_map/"
  val train_order_pt = train_pt + "/order_data/"
  val train_order_abs_pt = train_pt + "/order_abs_data/"
  val train_poi_pt = train_pt + "/poi_data/"
  val train_traffic_pt = train_pt + "/traffic_data/"
  val train_weather_pt = train_pt + "/weather_data/"
  val train_fs_pt = train_pt + "/fs/"
  val train_label_pt = train_pt + "/label/"
  val train_time_slices = train_pt + "/test_time_slices"

  val test1_pt = data_pt + "/test_set_1/"
  val test1_time_slices = test1_pt + "/test_time_slices"
  val test1_cluater_pt = test1_pt + "/cluster_map/"
  val test1_order_pt = test1_pt + "/order_data/"
  val test1_poi_pt = test1_pt + "/poi_data/"
  val test1_traffic_pt = test1_pt + "/traffic_data/"
  val test1_weather_pt = test1_pt + "/weather_data/"
}