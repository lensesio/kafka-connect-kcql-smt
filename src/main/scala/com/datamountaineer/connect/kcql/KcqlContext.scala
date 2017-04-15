package com.datamountaineer.connect.kcql

import com.datamountaineer.kcql.{Field, Kcql}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class KcqlContext(val fields: Seq[Field]) {

  private val cache = FieldsMapBuilder(fields)

  def getFieldsForPath(parents: Seq[String]): Seq[Either[Field, String]] = {
    val key = parents.mkString(".")
    cache.getOrElse(key, Seq.empty)
  }

  private object FieldsMapBuilder {
    private def insertKey(key: String,
                          item: Either[Field, String],
                          map: Map[String, ArrayBuffer[Either[Field, String]]]): Map[String, ArrayBuffer[Either[Field, String]]] = {

      map.get(key) match {
        case None =>
          val buffer = ArrayBuffer.empty[Either[Field, String]]
          buffer += item
          map + (key -> buffer)

        case Some(buffer) =>
          if (!buffer.contains(item)) {
            buffer += item
          }
          map
      }
    }

    def apply(fields: Seq[Field]): Map[String, Seq[Either[Field, String]]] = {
      fields.foldLeft(Map.empty[String, ArrayBuffer[Either[Field, String]]]) { case (map, field) =>
        if (field.hasParents) {
          val (_, m) = field.getParentFields
            .foldLeft((new StringBuilder(), map)) { case ((builder, accMap), p) =>
              val localMap = insertKey(builder.toString(), Right(p), accMap)
              if (builder.isEmpty) builder.append(p)
              builder.append(p) -> localMap
            }
          insertKey(field.getParentFields.mkString("."), Left(field), m)
        } else {
          insertKey("", Left(field), map)
        }
      }
    }

    def apply(kcql: Kcql): Map[String, Seq[Either[Field, String]]] = apply(kcql.getFields)
  }

}


/*case class KcqlContext(fields: Seq[Field]) {
  def getFieldsForPath(path: String): Seq[Field] = {
    fields.flatMap { f =>
      val prefix = Option(f.getParentFields)
        .map(_.mkString("."))
        .getOrElse("")

      if (prefix == path) Some(Left(f))
      else if (prefix.startsWith(path)) Some(Right(prefix.drop(path.length).takeWhile(_ != '.')))
      else None
    }
  }
}*/
