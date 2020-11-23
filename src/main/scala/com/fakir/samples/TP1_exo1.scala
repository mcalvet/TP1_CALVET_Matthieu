package com.fakir.samples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object TP1_exo1 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    println("Hello world")

    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    //Question 1
    val rdd = sparkSession.sparkContext.textFile("data/donnees.csv")

    //Question 2
    val films_DiCaprio = rdd.filter(elem => elem.contains("Di Caprio"))
    print("Nb de films de Di Caprio : " + films_DiCaprio.count() + "\n")

    //Question 3
    val notes_DiCaprio = films_DiCaprio.map(elem => (elem.split(";")(2).toDouble))
    val moyenne_notes = notes_DiCaprio.sum
    print("Moyenne des notes des films de Di Caprio : " + moyenne_notes / films_DiCaprio.count() + "\n")

    //Question 4
    val vues_tot_DiCaprio = films_DiCaprio.map(elem => (elem.split(";")(1).toDouble))
    val vues_tot = rdd.map(elem => (elem.split(";")(1).toDouble))
    val poucentage_vues_DiCaprio = vues_tot_DiCaprio.sum() / vues_tot.sum()
    println("Pourcentage de vues des films de Di Caprio dans l'échantillon : " + poucentage_vues_DiCaprio * 100 + "%")

    //Question 5
    val acteurs = rdd.map(item => (item.split(";")(3))).distinct.collect()
    acteurs.foreach(println)


  }
  }
