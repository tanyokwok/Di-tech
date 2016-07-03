package com.houjp.common.io

import com.houjp.common.Log

import scala.io.Source
import java.io._

object IO extends Log {

  /**
    * Load the file from disk.
    *
    * @param fp
    * @return
    */
  def load(fp: String): Array[String] = {
    log(s"load string-file($fp) from disk")
    Source.fromFile(fp).getLines().toArray
  }

  def write(fp: String, ls: Array[String]) = {
    log(s"write string-file($fp) to disk")
    val writer = new PrintWriter(new File(fp))

    ls.foreach { l =>
      writer.write(l + "\n")
    }

    writer.close()
  }

  /** Serialize a object to file */
  def write_obj[T <: Serializable](res_pt: String, obj: T): Unit = {
    log(s"write object-file($res_pt) from disk")
    val wf = new ObjectOutputStream(new FileOutputStream(res_pt))
    wf.writeObject(obj)
    wf.close()
  }

  /** Deserialize a object from a local file */
  def load_obj[T <: Serializable](pt: String): T = {
    log(s"load object-file($pt) from disk")
    val rf = new ObjectInputStream(new FileInputStream(pt))
    val v = rf.readObject()
    rf.close()
    v.asInstanceOf[T]
  }
}
