package ditech.common.util

import scala.reflect.io.Path

/**
  * Created by Administrator on 2016/5/26.
  */
object Directory {

  def create(path:Path): Unit ={
    while( !path.parent.exists ) create( path.parent )
    path.createDirectory()
  }
  def create(path:String){
    val dir = Path(path)
    create(dir)
  }
}
