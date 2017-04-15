package com.datamountaineer.connect.kcql

import java.util

import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}

case class Ingredient(name: String, sugar: Double, fat: Double)

case class Pizza(name: String,
                 ingredients: Seq[Ingredient],
                 vegetarian: Boolean,
                 vegan: Boolean,
                 calories: Int)
