package com.datamountaineer.connect.kcql

import java.nio.ByteBuffer

import com.datamountaineer.connect.kcql.StructKcql._
import com.datamountaineer.json.kcql.JacksonJson
import com.datamountaineer.json.kcql.JsonKcql._
import com.datamountaineer.kcql.Kcql
import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.data.{Schema, Struct}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

private object Transform extends StrictLogging {
  def apply(kcql: Kcql,
            schema: Schema,
            value: Any,
            isKey: Boolean,
            topic: String,
            partition: Int): (Schema, Any) = {
    def raiseException(msg: String, e: Option[Throwable]): (Schema, Any) = {
      val errMsg =
        s"""
           |$msg
           |Kcql
           | source=${kcql.getSource}; fiels=${kcql.getFields.map(_.getName).mkString(",")}
           |Record
           | topic=$topic; partition=$partition""".stripMargin

      e match {
        case None => logger.error(errMsg)
        case Some(ex) => logger.error(errMsg, ex)
      }
      throw new IllegalArgumentException(errMsg)
    }

    if (value == null) {
      if (schema == null || !schema.isOptional) {
        raiseException("Null value is not allowed.", None)
      }
      else schema -> value
    } else {

      if (schema != null) {
        schema.`type`() match {
          case Schema.Type.BYTES =>
            //we expected to be json
            val array = value match {
              case a: Array[Byte] => a
              case b: ByteBuffer => b.array()
              case other => raiseException("Invalid payload:$other for schema Schema.BYTES.", None)
                throw new IllegalArgumentException()
            }

            Try(JacksonJson.mapper.readTree(array)) match {
              case Failure(e) => raiseException("Invalid json.", Some(e))
              case Success(json) =>
                Try(json.kcql(kcql)) match {
                  case Failure(e) => raiseException(s"A KCQL exception occurred. ${e.getMessage}", Some(e))
                  case Success(jn) =>
                    schema -> jn.toString.getBytes("UTF-8")
                }
            }

          case Schema.Type.STRING =>
            //we expected to be json
            Try(JacksonJson.asJson(value.asInstanceOf[String])) match {
              case Failure(e) => raiseException("Invalid json", Some(e))
              case Success(json) =>
                Try(json.kcql(kcql)) match {
                  case Success(jn) => schema -> jn.toString
                  case Failure(e) => raiseException(s"A KCQL exception occurred.${e.getMessage}", Some(e))
                }
            }

          case Schema.Type.STRUCT =>
            val struct = value.asInstanceOf[Struct]
            Try(struct.kcql(kcql)) match {
              case Success(s) => s.schema() -> s
              case Failure(e) => raiseException(s"A KCQL error occurred.${e.getMessage}", Some(e))
            }

          case other => raiseException("Can't transform Schema type:$other.", None)
        }
      } else {
        //we can handle java.util.Map (this is what JsonConverter can spit out)
        value match {
          case m: java.util.Map[_, _] =>
            val map = m.asInstanceOf[java.util.Map[String, Any]]
            val jsonNode: JsonNode = JacksonJson.mapper.valueToTree(map)
            Try(jsonNode.kcql(kcql)) match {
              case Success(j) => schema -> JacksonJson.mapper.convertValue(j, classOf[java.util.Map[String, Any]])
              case Failure(e) => raiseException(s"A KCQL exception occurred.${e.getMessage}", Some(e))
            }
          case s: String =>
            Try(JacksonJson.asJson(value.asInstanceOf[String])) match {
              case Failure(e) => raiseException("Invalid json", Some(e))
              case Success(json) =>
                Try(json.kcql(kcql)) match {
                  case Success(jn) => schema -> jn.toString
                  case Failure(e) => raiseException(s"A KCQL exception occurred.${e.getMessage}", Some(e))
                }
            }

          case b: Array[Byte] =>
            Try(JacksonJson.mapper.readTree(b)) match {
              case Failure(e) => raiseException("Invalid json.", Some(e))
              case Success(json) =>
                Try(json.kcql(kcql)) match {
                  case Failure(e) => raiseException(s"A KCQL exception occurred. ${e.getMessage}", Some(e))
                  case Success(jn) => schema -> jn.toString.getBytes("UTF-8")
                }
            }
          //we take it as String
          case other => raiseException(s"Value:$other is not handled!", None)
        }
      }
    }
  }
}
