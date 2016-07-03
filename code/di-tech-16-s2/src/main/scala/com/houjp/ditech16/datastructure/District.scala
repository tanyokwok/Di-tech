package com.houjp.ditech16.datastructure

import com.houjp.common.io.IO

class District(val district_hash: String, val district_id: Int, val type_id:Int) {

}

object District {

  def parse(s: String): District = {
    val Array(d_hash, d_id_s, type_s) = s.split("\t")
    new District(d_hash, d_id_s.toInt, type_s.toInt)
  }

  def load_local(fp: String): Map[String, Int] = {
    IO.load(fp).map(parse).map(e => (e.district_hash, e.district_id)).toMap
  }

  def loadDidTypeId(fp: String): Map[String, (Int, Int)] = {
    IO.load(fp).map(parse).map(e => (e.district_hash, (e.district_id, e.type_id ))).toMap
  }
}