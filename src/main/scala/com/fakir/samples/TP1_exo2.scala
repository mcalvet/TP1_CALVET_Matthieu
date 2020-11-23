package com.fakir.samples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object TP1_exo2 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    println("Hello world")

    val sparkSession = SparkSession.builder().master("local").getOrCreate()
    val df: DataFrame = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("data/donnees.csv")


    val renamed_df: DataFrame = df.withColumnRenamed("_c0", "titre_film")
      .withColumnRenamed("_c1", "nb_vues")
      .withColumnRenamed("_c2", "note")
      .withColumnRenamed("_c3", "acteur")


    val films_DiCaprio: DataFrame = renamed_df.filter(renamed_df("acteur") === "Di Caprio")
    print("Il y a " + films_DiCaprio.count() + " films de Leonardo Di Caprio")

    val moyenne_notes_DiCaprio: DataFrame = films_DiCaprio.groupBy( col1 = "acteur").mean( colNames = "note")
    moyenne_notes_DiCaprio.show

    val vues_tot  = renamed_df.agg(sum("nb_vues")).first.get(0).toString.toDouble
    val vues_tot_DiCaprio = films_DiCaprio.agg(sum("nb_vues")).first.get(0).toString.toDouble

    val pourcentage_vues_DiCaprio: Double = vues_tot_DiCaprio / vues_tot * 100
    print("Pourcentage de vues des films de Di Caprio : " + pourcentage_vues_DiCaprio)

    val moyenne_notes_acteur = renamed_df.groupBy( col1 = "acteur").mean( colNames = "nb_vues")
    moyenne_notes_acteur.show

    val moyenne_vues_acteur = renamed_df.groupBy( col1 = "acteur").mean( colNames = "note")
    moyenne_vues_acteur.show

    val pourcentage_vues = renamed_df.withColumn(colName = "pourcentage_de_vues", col(colName = "nb_vues") / vues_tot * 100)
    pourcentage_vues.show

  }
}

