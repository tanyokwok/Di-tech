package com.houjp

package object ditech16 {

  val is_local = true

  val max_time_id = 144
  val max_new_time_id = 1440

  // HDFS

  // Mac

  // Windows

  val project_pt = "/home/houjp/di-tech-16/"
  val data_pt = project_pt + "/data/season_2/"
  val ans_pt = project_pt + "/data/ans/"
  val cluster_pt = data_pt + "/cluster_map/"
  val poi_pt = data_pt + "/poi_data/"
  val sample_key_pt = data_pt + "/sample_key"

  val offline_pt = data_pt + "/offline/"
  val train_order_pt = offline_pt + "/order_data/"
  val train_order_abs_pt = offline_pt + "/order_abs_data/"
  val train_traffic_pt = offline_pt + "/traffic_data/"
  val train_weather_pt = offline_pt + "/weather_data/"
  val train_fs_pt = offline_pt + "/fs/"
  val train_label_pt = offline_pt + "/label/"
  val train_time_slices = offline_pt + "/test_time_slices"

  val online_pt = data_pt + "/online/"
  val test1_time_slices = online_pt + "/test_time_slices"
  val test1_order_pt = online_pt + "/order_data/"
  val test1_traffic_pt = online_pt + "/traffic_data/"
  val test1_weather_pt = online_pt + "/weather_data/"
}