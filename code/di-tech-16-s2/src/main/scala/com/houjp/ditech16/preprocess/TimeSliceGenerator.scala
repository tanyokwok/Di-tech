package com.houjp.ditech16.preprocess

import com.houjp.common.io.IO
import com.houjp.ditech16
import com.houjp.ditech16.datastructure.TimeSlice

object TimeSliceGenerator {

  def main(args: Array[String]) {
//    run_big(ditech16.data_pt)
//    run_small(ditech16.data_pt)
//    run_median(ditech16.data_pt)
//    run_near(args(0), args(1).toInt, args(2).toInt)
    run_offline(args(0), args(1).toInt, args(2).toInt)
  }

  def run_near(nts_fp: String, offset: Int, range: Int): Unit = {
    // save offline train_new_time_slices
    val offline_train_new_time_slices_fp = nts_fp
    val offline_train_new_time_slices = (Range(1, 21 + 1).flatMap { day =>
      Range(-1 * range, range + 1).map { off =>
        f"2016-01-$day%02d-${451 + off + offset}"
      } ++ Range(-10 , 10).map { off =>
        f"2016-01-$day%02d-${571 + off + offset}"
      } ++ Range(-10, 10).map { off =>
        f"2016-01-$day%02d-${691 + off + offset}"
      } ++ Range(-10, 10).map { off =>
        f"2016-01-$day%02d-${811 + off + offset}"
      } ++ Range(-10, 10).map { off =>
        f"2016-01-$day%02d-${931 + off + offset}"
      } ++ Range(-10, 10).map { off =>
        f"2016-01-$day%02d-${1051 + off + offset}"
      } ++ Range(-10, 10).map { off =>
        f"2016-01-$day%02d-${1171 + off + offset}"
      } ++ Range(-10, 10).map { off =>
        f"2016-01-$day%02d-${1291 + off + offset}"
      } ++ Range(-10, 10).map { off =>
        f"2016-01-$day%02d-${1411 + off + offset}"
      }

    } ++ Range(23, 29 + 1).flatMap { day =>
      Range(-1 * range, range + 1).map { off =>
        f"2016-02-$day%02d-${451 + off + offset}"
      } ++ Range(-10, 10).map { off =>
        f"2016-02-$day%02d-${571 + off + offset}"
      } ++ Range(-10, 10).map { off =>
        f"2016-02-$day%02d-${691 + off + offset}"
      } ++ Range(-10, 10).map { off =>
        f"2016-02-$day%02d-${811 + off + offset}"
      } ++ Range(-10, 10).map { off =>
        f"2016-02-$day%02d-${931 + off + offset}"
      } ++ Range(-10, 10).map { off =>
        f"2016-02-$day%02d-${1051 + off + offset}"
      } ++ Range(-10, 10).map { off =>
        f"2016-02-$day%02d-${1171 + off + offset}"
      } ++ Range(-10, 10).map { off =>
        f"2016-02-$day%02d-${1291 + off + offset}"
      } ++ Range(-10, 10).map { off =>
        f"2016-02-$day%02d-${1411 + off + offset}"
      }
    } ++ Range(1, 10 + 1).flatMap { day =>
      Range(-1 * range, range + 1).map { off =>
        f"2016-03-$day%02d-${451 + off + offset}"
      } ++ Range(-10, 10).map { off =>
        f"2016-03-$day%02d-${571 + off + offset}"
      } ++ Range(-10, 10).map { off =>
        f"2016-03-$day%02d-${691 + off + offset}"
      } ++ Range(-10, 10).map { off =>
        f"2016-03-$day%02d-${811 + off + offset}"
      } ++ Range(-10, 10).map { off =>
        f"2016-03-$day%02d-${931 + off + offset}"
      } ++ Range(-10, 10).map { off =>
        f"2016-03-$day%02d-${1051 + off + offset}"
      } ++ Range(-10, 10).map { off =>
        f"2016-03-$day%02d-${1171 + off + offset}"
      } ++ Range(-10, 10).map { off =>
        f"2016-03-$day%02d-${1291 + off + offset}"
      } ++ Range(-10, 10).map { off =>
        f"2016-03-$day%02d-${1411 + off + offset}"
      }
    }).toArray
    IO.write(offline_train_new_time_slices_fp, offline_train_new_time_slices)
  }

  def run_median(data_pt: String): Unit = {
    // save offline train_new_time_slices
    val offline_train_new_time_slices_fp = data_pt + "/offline/train_new_time_slices"
    val offline_train_new_time_slices = (Range(1, 21 + 1).flatMap { day =>
      Range(1, 1431 + 1).map {new_tid =>
        f"2016-01-$day%02d-$new_tid"
      }
    } ++ Range(23, 29 + 1).flatMap { day =>
      Range(1, 1431 + 1).map {new_tid =>
        f"2016-02-$day%02d-$new_tid"
      }
    } ++ Range(1, 10 + 1).flatMap { day =>
      Range(1, 1431 + 1).map {new_tid =>
        f"2016-03-$day%02d-$new_tid"
      }
    }).toArray
    IO.write(offline_train_new_time_slices_fp, offline_train_new_time_slices)
  }

  def run_small(data_pt: String): Unit = {
    // save offline train_new_time_slices
    val offline_train_new_time_slices_fp = data_pt + "/offline/train_new_time_slices"
    val offline_train_new_time_slices = (Range(1, 21 + 1).flatMap { day =>
      Range(1, 144 + 1).map {new_tid =>
        f"2016-01-$day%02d-${(new_tid - 1) * 10 + 1}"
      }
    } ++ Range(23, 29 + 1).flatMap { day =>
      Range(1, 144 + 1).map {new_tid =>
        f"2016-02-$day%02d-${(new_tid - 1) * 10 + 1}"
      }
    } ++ Range(1, 10 + 1).flatMap { day =>
      Range(1, 144 + 1).map {new_tid =>
        f"2016-03-$day%02d-${(new_tid - 1) * 10 + 1}"
      }
    }).toArray
    IO.write(offline_train_new_time_slices_fp, offline_train_new_time_slices)

  }

  def run_offline(nts_fp: String, offset: Int, range: Int): Unit = {
    val train_new_time_slices = (Range(1, 21 + 1).flatMap { day =>
      Range(-1 * range, range + 1).map { off =>
        f"2016-01-$day%02d-${451 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-01-$day%02d-${571 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-01-$day%02d-${691 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-01-$day%02d-${811 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-01-$day%02d-${931 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-01-$day%02d-${1051 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-01-$day%02d-${1171 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-01-$day%02d-${1291 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-01-$day%02d-${1411 + off + offset}"
      }

    } ++ Range(23, 29 + 1).flatMap { day =>
      Range(-1 * range, range + 1).map { off =>
        f"2016-02-$day%02d-${451 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-02-$day%02d-${571 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-02-$day%02d-${691 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-02-$day%02d-${811 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-02-$day%02d-${931 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-02-$day%02d-${1051 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-02-$day%02d-${1171 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-02-$day%02d-${1291 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-02-$day%02d-${1411 + off + offset}"
      }
    } ++ Range(1, 10 + 1).flatMap { day =>
      Range(-1 * range, range + 1).map { off =>
        f"2016-03-$day%02d-${451 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-03-$day%02d-${571 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-03-$day%02d-${691 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-03-$day%02d-${811 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-03-$day%02d-${931 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-03-$day%02d-${1051 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-03-$day%02d-${1171 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-03-$day%02d-${1291 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-03-$day%02d-${1411 + off + offset}"
      }
    }).toArray
    IO.write(nts_fp, train_new_time_slices)
  }

  def run_online(offset: Int, range: Int): Unit = {
    // load test_old_time_slices
    val online_test_time_slices = TimeSlice.load_old(ditech16.data_pt + "/online/std_test_time_slices")

    // new time slice ids
    val new_tids = online_test_time_slices.map(_.new_time_id).distinct

    // save online test_new_time_slices
    val online_test_new_time_slices_fp = ditech16.data_pt + "/online/test_new_time_slices"
    IO.write(online_test_new_time_slices_fp, online_test_time_slices.map(_.toString))

    // save online train_new_time_slices
    val online_train_new_time_slices_fp = ditech16.data_pt + "/online/train_new_time_slices"
    val online_train_new_time_slices = (Range(1, 21 + 1).flatMap { day =>
      Range(-1 * range, range + 1).map { off =>
        f"2016-01-$day%02d-${451 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-01-$day%02d-${571 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-01-$day%02d-${691 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-01-$day%02d-${811 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-01-$day%02d-${931 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-01-$day%02d-${1051 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-01-$day%02d-${1171 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-01-$day%02d-${1291 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-01-$day%02d-${1411 + off + offset}"
      }

    } ++ Range(23, 29 + 1).flatMap { day =>
      Range(-1 * range, range + 1).map { off =>
        f"2016-02-$day%02d-${451 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-02-$day%02d-${571 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-02-$day%02d-${691 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-02-$day%02d-${811 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-02-$day%02d-${931 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-02-$day%02d-${1051 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-02-$day%02d-${1171 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-02-$day%02d-${1291 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-02-$day%02d-${1411 + off + offset}"
      }
    } ++ Range(1, 17 + 1).flatMap { day =>
      Range(-1 * range, range + 1).map { off =>
        f"2016-03-$day%02d-${451 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-03-$day%02d-${571 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-03-$day%02d-${691 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-03-$day%02d-${811 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-03-$day%02d-${931 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-03-$day%02d-${1051 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-03-$day%02d-${1171 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-03-$day%02d-${1291 + off + offset}"
      } ++ Range(-1 * range, range + 1).map { off =>
        f"2016-03-$day%02d-${1411 + off + offset}"
      }
    }).toArray
    IO.write(online_train_new_time_slices_fp, online_train_new_time_slices)
  }


  def run_big(data_pt: String): Unit = {

    // load test_old_time_slices
    val online_test_time_slices = TimeSlice.load_old(data_pt + "/online/test_old_time_slices")

    // new time slice ids
    val new_tids = online_test_time_slices.map(_.new_time_id).distinct

    // save online test_new_time_slices
    val online_test_new_time_slices_fp = data_pt + "/online/test_new_time_slices"
    IO.write(online_test_new_time_slices_fp, online_test_time_slices.map(_.toString))

    // save offline test_new_time_slices 0 / -10 / 1-
    var offset = 0
    var offline_test_new_time_slices_fp = data_pt + s"/offline/test_new_time_slices_$offset"
    var offline_test_new_time_slices = Range(11, 18).flatMap { day =>
      new_tids.map { new_tid =>
        f"2016-03-$day%02d-${new_tid + offset}%d"
      }
    }.toArray
    IO.write(offline_test_new_time_slices_fp, offline_test_new_time_slices)

    offset = -10
    offline_test_new_time_slices_fp = data_pt + s"/offline/test_new_time_slices_$offset"
    offline_test_new_time_slices = Range(11, 18).flatMap { day =>
      new_tids.map { new_tid =>
        f"2016-03-$day%02d-${new_tid + offset}%d"
      }
    }.toArray
    IO.write(offline_test_new_time_slices_fp, offline_test_new_time_slices)

    offset = 10
    offline_test_new_time_slices_fp = data_pt + s"/offline/test_new_time_slices_$offset"
    offline_test_new_time_slices = Range(11, 18).flatMap { day =>
      new_tids.map { new_tid =>
        f"2016-03-$day%02d-${new_tid + offset}%d"
      }
    }.toArray
    IO.write(offline_test_new_time_slices_fp, offline_test_new_time_slices)

    // save online train_new_time_slices
    val online_train_new_time_slices_fp = data_pt + "/online/train_new_time_slices"
    val online_train_new_time_slices = (Range(31, 1440 + 1).map { new_tid =>
      s"2016-01-01-$new_tid"
    } ++ Range(2, 20 + 1).flatMap { day =>
      Range(1, 1440 + 1).map { new_tid =>
        f"2016-01-$day%02d-$new_tid"
      }
    } ++ Range(1, 1431 + 1).map { new_tid =>
      s"2016-01-21-$new_tid"
    } ++ Range(31, 1440 + 1).map { new_tid =>
      s"2016-02-23-$new_tid"
    } ++ Range(24, 29 + 1).flatMap { day =>
      Range(1, 1440 + 1).map { new_tid =>
        f"2016-02-$day%02d-$new_tid"
      }
    } ++ Range(1, 16 + 1).flatMap { day =>
      Range(1, 1440 + 1).map { new_tid =>
        f"2016-03-$day%02d-$new_tid"
      }
    } ++ Range(1, 1431 + 1).map { new_tid =>
      s"2016-03-17-$new_tid"
    }).toArray
    IO.write(online_train_new_time_slices_fp, online_train_new_time_slices)

    // save offline train_new_time_slices
    val offline_train_new_time_slices_fp = data_pt + "/offline/train_new_time_slices"
    val offline_train_new_time_slices = (Range(31, 1440 + 1).map { new_tid =>
      s"2016-01-01-$new_tid"
    } ++ Range(2, 20 + 1).flatMap { day =>
      Range(1, 1440 + 1).map { new_tid =>
        f"2016-01-$day%02d-$new_tid"
      }
    } ++ Range(1, 1431 + 1).map { new_tid =>
      s"2016-01-21-$new_tid"
    } ++ Range(31, 1440 + 1).map { new_tid =>
      s"2016-02-23-$new_tid"
    } ++ Range(24, 29 + 1).flatMap { day =>
      Range(1, 1440 + 1).map { new_tid =>
        f"2016-02-$day%02d-$new_tid"
      }
    } ++ Range(1, 9 + 1).flatMap { day =>
      Range(1, 1440 + 1).map { new_tid =>
        f"2016-03-$day%02d-$new_tid"
      }
    } ++ Range(1, 1431 + 1).map { new_tid =>
      s"2016-03-10-$new_tid"
    }).toArray
    IO.write(offline_train_new_time_slices_fp, offline_train_new_time_slices)

  }
}