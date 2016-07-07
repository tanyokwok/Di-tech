package ditech.feature

/**
  * Created by lvfuyu on 16/6/27.
  */
object FineArriveSmooth {

  def main (args: Array[String]): Unit = {

    val fs_names = Array(
      FineArrive.getClass().getSimpleName.replace("$", "")
    )
    FSmooth.run(fs_names, FineArriveSmooth.getClass().getSimpleName.replace("$", ""))
  }

}
