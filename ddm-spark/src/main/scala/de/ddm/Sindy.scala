package de.ddm

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.rdd.RDD
import java.io.{File, BufferedWriter, FileWriter}

object Sindy {

  private def readData(input: String, spark: SparkSession): Dataset[Row] = {
    spark
      .read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(input)
  }

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    println(s"\n[INFO] Starting Inclusion Dependency Discovery for ${inputs.length} datasets.")

    // Step 1: Read datasets into DataFrames
    val tables = inputs.map(input => readData(input, spark))
    println(s"[INFO] Successfully loaded ${tables.length} datasets.")

    // Step 2: Extract (value, column) pairs
    val valueColumnPairs: RDD[(String, String)] = spark.sparkContext.union(
      tables.flatMap(df =>
        df.columns.map(col => {
          df.select(col).rdd
            .flatMap(r => Option(r.getString(0)).map(_.trim).filter(_.nonEmpty).map(v => (v, col)))
        })
      )
    ).persist()

    println("[DEBUG] Sample value-column pairs:")
    valueColumnPairs.take(10).foreach(println)

    // Step 3: Group by values to create attribute sets (value -> set of columns containing it)
    val attributeSets: RDD[Set[String]] = valueColumnPairs
      .groupByKey()
      .map { case (_, columns) => columns.toSet }
      .persist()

    println("[DEBUG] Sample attribute sets:")
    attributeSets.take(10).foreach(println)

    // Step 4: Create Inclusion Dependencies list (column -> set of possible supersets)
    val inclusionList: RDD[(String, Set[String])] = attributeSets.flatMap(set =>
      set.map(column => (column, set - column))
    )

    println("[DEBUG] Sample inclusion dependencies (before filtering):")
    inclusionList.take(10).foreach(println)

    // Step 5: Apply strict intersection to remove false positives
    val strictINDs: RDD[(String, Set[String])] = inclusionList
      .reduceByKey(_ intersect _)
      .mapValues(_.toSeq.sorted.toSet)
      .filter { case (dep, ref) => ref.nonEmpty }
//      .distinct()
      .sortByKey(ascending = true)
      .persist()

    println("[DEBUG] Sample strict INDs (after intersection & filtering):")
    strictINDs.take(10).foreach(println)

    val finalINDCount = strictINDs.count()
    println(s"[INFO] Computed $finalINDCount valid Inclusion Dependencies.")
    if (finalINDCount == 0) {
      println("[ERROR] No valid INDs found. Possible causes: No intersections, overly strict filtering.")
      return
    }

    // Step 6: Write results to `results.txt`
    val outputFile = new File("results.txt")
    val writer = new BufferedWriter(new FileWriter(outputFile))

    println("\n[INFO] Writing results to results.txt...")
    var count = 0

    strictINDs
      .sortBy(_._1)
      .collect()
      .foreach { case (dependent, referenced) =>
      val sortedReferenced = referenced.toSeq.sorted
      val indString = s"$dependent < ${sortedReferenced.mkString(", ")}"
      writer.write(indString + "\n")
      count += 1
      println(s"[DEBUG] Writing IND #$count: $indString")
    }

    writer.close()
    println(s"\n[INFO] Inclusion Dependency Discovery Completed. Results saved to results.txt.")
  }
}
