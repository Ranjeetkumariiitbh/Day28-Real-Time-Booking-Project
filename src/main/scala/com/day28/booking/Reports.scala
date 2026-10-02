package com.day28.booking

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Reports {

  type OccKey = (String, String)          // (category, resourceId)
  type OccVal = (Int, Int, Int)           // (occupiedUnits, activeBookings, cancelledBookings)

  def sum3(a: OccVal, b: OccVal): OccVal = (a._1 + b._1, a._2 + b._2, a._3 + b._3)

  def status(capacity: Int, occupied: Int, pct: Double): String =
    if (occupied > capacity) "OVERBOOKED"
    else if (occupied == capacity) "SOLD_OUT"
    else if (pct >= 80.0) "LOW_AVAILABILITY"
    else "AVAILABLE"

  /**
   * Pair RDD -> one OccupancyRow for EVERY reference resource (resources with no
   * bookings are added with zero counts so availability is always reported).
   * Capacity / price / name / city come from the broadcast reference map.
   */
  def occupancyRows(rdd: RDD[(OccKey, OccVal)], ref: Broadcast[Map[String, Resource]]): RDD[OccupancyRow] = {
    val zeros: Seq[(OccKey, OccVal)] =
      ref.value.values.toSeq.map(r => ((r.category, r.resourceId), (0, 0, 0)))
    rdd.union(rdd.sparkContext.parallelize(zeros))
      .reduceByKey(sum3 _)
      .map { case ((category, id), (occupied, active, cancelled)) =>
        val r   = ref.value(id)
        val pct = math.round(occupied * 1000.0 / r.capacity) / 10.0
        OccupancyRow(category, id, r.name, r.city, r.capacity, occupied,
          math.max(0, r.capacity - occupied), pct, active, cancelled,
          occupied * r.unitPrice, status(r.capacity, occupied, pct))
      }
  }

  def windowRows(rdd: RDD[(OccKey, WindowStats)], ref: Broadcast[Map[String, Resource]]): RDD[WindowRow] =
    rdd.map { case ((category, id), s) =>
      val r = ref.value(id)
      WindowRow(category, id, r.name, r.city, s.bookings, s.cancellations, s.bookedUnits, s.cancelledUnits)
    }

  private def section(title: String, df: DataFrame): Unit = {
    println(s"[$title]")
    df.show(100, truncate = false)
  }

  def runOccupancyReports(spark: SparkSession, df: DataFrame, outputDir: String): Unit = {
    df.createOrReplaceTempView("occupancy")

    section("REPORT 1 - Occupancy & availability by category",
      spark.sql(
        """SELECT category,
          |       COUNT(*)                         AS resources,
          |       SUM(capacity)                    AS capacity,
          |       SUM(occupiedUnits)               AS occupied,
          |       SUM(availableUnits)              AS available,
          |       ROUND(CAST(SUM(occupiedUnits) AS DOUBLE) * 100 / SUM(capacity), 1) AS occupancyPct,
          |       SUM(activeBookings)              AS activeBookings,
          |       SUM(cancelledBookings)           AS cancelledBookings,
          |       ROUND(SUM(revenue), 2)           AS revenue
          |FROM occupancy
          |GROUP BY category
          |ORDER BY category""".stripMargin))

    section("REPORT 2 - Top 5 most occupied resources",
      spark.sql(
        """SELECT category, resourceId, name, capacity, occupiedUnits, availableUnits, occupancyPct, status
          |FROM occupancy
          |ORDER BY occupancyPct DESC, resourceId
          |LIMIT 5""".stripMargin))

    section("REPORT 3 - Availability alerts",
      spark.sql(
        """SELECT category, resourceId, name, capacity, occupiedUnits, availableUnits, occupancyPct, status
          |FROM occupancy
          |WHERE status IN ('OVERBOOKED', 'SOLD_OUT', 'LOW_AVAILABILITY')
          |ORDER BY occupancyPct DESC, resourceId""".stripMargin))

    section("REPORT 4 - Revenue by city",
      spark.sql(
        """SELECT city,
          |       SUM(occupiedUnits)     AS occupiedUnits,
          |       ROUND(SUM(revenue), 2) AS revenue
          |FROM occupancy
          |GROUP BY city
          |ORDER BY revenue DESC, city""".stripMargin))

    if (outputDir.nonEmpty) {
      df.orderBy("category", "resourceId")
        .coalesce(1)
        .write.mode("overwrite").option("header", "true")
        .csv(s"$outputDir/occupancy_snapshot")
    }
  }

  def runWindowReports(spark: SparkSession, df: DataFrame, label: String): Unit = {
    df.createOrReplaceTempView("window_activity")

    section(s"WINDOW $label - Activity by category",
      spark.sql(
        """SELECT category,
          |       SUM(bookings)       AS bookings,
          |       SUM(cancellations)  AS cancellations,
          |       SUM(bookedUnits)    AS bookedUnits,
          |       SUM(cancelledUnits) AS cancelledUnits,
          |       SUM(bookedUnits) - SUM(cancelledUnits) AS netUnits,
          |       ROUND(CAST(SUM(cancellations) AS DOUBLE) * 100 / NULLIF(SUM(bookings), 0), 1) AS cancelRatePct
          |FROM window_activity
          |GROUP BY category
          |ORDER BY category""".stripMargin))

    section(s"WINDOW $label - Hot resources (top 3 by booked units)",
      spark.sql(
        """SELECT category, resourceId, name, bookings, bookedUnits
          |FROM window_activity
          |WHERE bookedUnits > 0
          |ORDER BY bookedUnits DESC, resourceId
          |LIMIT 3""".stripMargin))
  }
}
