package ditech.feature

/**
  * Created by lvfuyu on 16/6/27.
  */
object FineGapSmooth {

  def main (args: Array[String]): Unit = {

    val fs_names = Array(
      FineGap.getClass().getSimpleName.replace("$", "")
    )
    FSmooth.run(fs_names, FineGapSmooth.getClass().getSimpleName.replace("$", ""))
  }

}
