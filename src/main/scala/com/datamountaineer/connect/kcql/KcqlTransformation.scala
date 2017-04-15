package com.datamountaineer.connect.kcql

import java.util

import com.datamountaineer.kcql.Kcql
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
import org.apache.kafka.connect.connector.ConnectRecord

/**
  * The KCQL transformer. It takes two kcql entries for keys and values.
  * @tparam T
  */
class Transformation[T <: ConnectRecord[T]] extends org.apache.kafka.connect.transforms.Transformation[T] {
  private var kcqlKeyMap = Map.empty[String, Kcql]
  private var kcqlValueMap = Map.empty[String, Kcql]

  override def apply(record: T): T = {
    val topic = record.topic()

    kcqlKeyMap.get(topic).map { kcql =>
      val (keySchema, keyValue) = Transform(kcql, record.keySchema(), record.key(), true, topic, record.kafkaPartition())
      kcqlValueMap.get(topic).map { kcql =>
        val (valueSchema, value) = Transform(kcql, record.valueSchema(), record.value(), false, topic, record.kafkaPartition())
        record.newRecord(
          record.topic(),
          record.kafkaPartition(),
          keySchema,
          keyValue,
          valueSchema,
          value,
          record.timestamp()
        )
      }.getOrElse {
        record.newRecord(
          record.topic(),
          record.kafkaPartition(),
          keySchema,
          keyValue,
          record.valueSchema(),
          record.value(),
          record.timestamp())
      }
    }.getOrElse {
      kcqlValueMap.get(topic).map { kcql =>
        val (valueSchema, value) = Transform(kcql, record.valueSchema(), record.value(), false, topic, record.kafkaPartition())
        record.newRecord(
          topic,
          record.kafkaPartition(),
          record.keySchema(),
          record.key(),
          valueSchema,
          value,
          record.timestamp()
        )
      }.getOrElse(record)
    }
  }

  override def config(): ConfigDef = Transformation.configDef

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {
    val config = new TransformationConfig(configs)

    def fromConfig(kcqlValue: String) =
      Option(kcqlValue)
        .map { c =>
          c.split(";")
            .map { k =>
              val kcql = Kcql.parse(k.trim)
              kcql.getSource -> kcql
            }.toMap
        }.getOrElse(Map.empty)

    kcqlKeyMap = fromConfig(config.getString(Transformation.KEY_KCQL_CONFIG))
    kcqlValueMap = fromConfig(config.getString(Transformation.VALUE_KCQL_CONFIG))
  }
}


private object Transformation {

  val KEY_KCQL_CONFIG = "connect.transforms.kcql.key"
  private val KEY_KCQL_DOC = "Provides the kcql transformation for the keys. To provide more than one separate them by ';'"
  private val KEY_KCQL_DISPLAY = "Key(-s) KCQL"

  val VALUE_KCQL_CONFIG = "connect.transforms.kcql.value"
  private val VALUE_KCQL_DOC = "Provides the kcql transformation for the Kafka message value. To provide more than one separate them by ';'"
  private val VALUE_KCQL_DISPLAY = "Value(-s) KCQL"


  val configDef: ConfigDef = new ConfigDef()
    .define(VALUE_KCQL_CONFIG,
      ConfigDef.Type.STRING,
      null,
      ConfigDef.Importance.MEDIUM,
      VALUE_KCQL_DOC,
      "Transforms",
      1,
      ConfigDef.Width.LONG,
      VALUE_KCQL_DISPLAY
    )
    .define(KEY_KCQL_CONFIG,
      ConfigDef.Type.STRING,
      null,
      ConfigDef.Importance.MEDIUM,
      KEY_KCQL_DOC,
      "Transforms",
      2,
      ConfigDef.Width.LONG,
      KEY_KCQL_DISPLAY
    )
}


class TransformationConfig(config: util.Map[String, _]) extends AbstractConfig(Transformation.configDef, config)