package hydra.common.util

object ClassUtils {

  // Null safe access to class.getName
  def getSimpleName(cls: Class[_]): String = {
    if (cls == null) "" else cls.getSimpleName
  }
}
