package com.houjp.common

import java.util.Date

trait Log {

  def log(msg: String) = {
    val t = new Date()
    println(s"$t [INFO] $msg")
  }
}