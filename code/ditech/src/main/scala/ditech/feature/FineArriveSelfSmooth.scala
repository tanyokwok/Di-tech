package ditech.feature

import com.houjp.common.io.IO
import com.houjp.ditech16
import ditech.common.util.Directory

/**
  * Created by lvfuyu on 16/6/27.
  */
object FineArriveSelfSmooth {

  def main (args: Array[String]): Unit = {

    val fs_names = Array(
      FineArriveSelf.getClass().getSimpleName.replace("$", "")
    )
    FSmooth.run(fs_names, FineArriveSelfSmooth.getClass().getSimpleName.replace("$", ""))
  }
}
