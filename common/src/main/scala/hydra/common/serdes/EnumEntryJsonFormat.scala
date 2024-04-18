package hydra.common.serdes

import enumeratum.EnumEntry
import spray.json.{DeserializationException, JsString, JsValue, RootJsonFormat}

class EnumEntryJsonFormat[E <: EnumEntry](values: Seq[E]) extends RootJsonFormat[E] {

  override def write(obj: E): JsValue = JsString(obj.entryName)

  override def read(json: JsValue): E = json match {
    case s: JsString => values.find(v => v.entryName == s.value).getOrElse(deserializationError(s))
    case x => deserializationError(x)
  }

  private def deserializationError(value: JsValue) = {
    val className = values.headOption.map(_.getClass.getEnclosingClass.getSimpleName).getOrElse("")
    throw DeserializationException(
      s"For '$className': Expected a value from enum $values instead of $value")
  }
}
