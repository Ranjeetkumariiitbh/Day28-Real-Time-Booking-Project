package com.day28.booking

import org.apache.spark.SparkContext

object ReferenceData {
  /** Load resources.csv on the driver; the caller broadcasts the resulting Map. */
  def load(sc: SparkContext, path: String): Map[String, Resource] =
    sc.textFile(path)
      .map(_.trim)
      .filter(l => l.nonEmpty && !l.startsWith("resource_id"))
      .map { line =>
        val p = line.split(",", -1).map(_.trim)
        Resource(p(0), p(1).toUpperCase, p(2), p(3), p(4).toInt, p(5).toDouble)
      }
      .collect()
      .map(r => r.resourceId -> r)
      .toMap
}
