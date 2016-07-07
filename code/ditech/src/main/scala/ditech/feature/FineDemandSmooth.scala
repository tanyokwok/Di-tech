package ditech.feature

/**
  * Created by lvfuyu on 16/6/27.
  */
object FineDemandSmooth {

  def main (args: Array[String]): Unit = {

    val fs_names = Array(
      FineDemand.getClass().getSimpleName.replace("$", "")
    )
    FSmooth.run(fs_names, FineDemandSmooth.getClass().getSimpleName.replace("$", ""))
  }
}
