package com.houjp.common.io

import org.scalatest.FunSuite
import com.houjp.ditech16

class IOTest extends FunSuite {

  val s = ditech16.test1_time_slices
  IO.load(s).foreach(println)
}