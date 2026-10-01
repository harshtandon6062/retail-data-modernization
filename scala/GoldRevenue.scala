// Monthly revenue from the gold star schema - the same logic as
// sql/analytics/01_monthly_revenue_trend.sql, but written in Scala.
//
// Run:  make scala      (uses the spark-shell bundled with pip PySpark)
//   = echo ':paste scala/GoldRevenue.scala' | spark-shell --packages io.delta:delta-spark_2.13:4.0.1 ...
//   Interactively you can also start spark-shell and type  :load scala/GoldRevenue.scala
//   (`spark-shell -i` is ignored by Spark 4's Scala 2.13 REPL when not run from a terminal.)
//
// Why Scala? Spark itself is written in Scala, so the Scala API is the "native"
// one: compile-time type checks and no Python<->JVM hop for UDFs. The DataFrame
// code below compiles to exactly the same execution plan as the PySpark version.

import org.apache.spark.sql.functions._

val lake = sys.env.getOrElse("LAKE_ROOT", "lake")

// Read Delta tables straight from their folders (no metastore needed)
val fact    = spark.read.format("delta").load(s"$lake/gold/fact_sales")
val dimDate = spark.read.format("delta").load(s"$lake/gold/dim_date")

val monthly = fact
  .filter(!col("order_status").isin("CANCELLED", "RETURNED"))     // revenue = kept orders only
  .join(dimDate, Seq("date_key"))                                  // star join on the date key
  .groupBy("year_month")
  .agg(
    round(sum("net_amount"), 2).as("revenue"),
    countDistinct("order_id").as("orders"))
  .orderBy("year_month")

println("=== Monthly revenue (Scala) ===")
monthly.show(30, truncate = false)

// Collect the small yearly result to the driver and format it with Scala string interpolation
val yearly = fact.filter(!col("order_status").isin("CANCELLED", "RETURNED"))
  .join(dimDate, Seq("date_key"))
  .groupBy("year").agg(round(sum("net_amount"), 2).as("revenue"))
  .orderBy("year")
  .collect()
yearly.foreach { r =>
  val year: Int = r.getAs[Int]("year")
  val revenue: java.math.BigDecimal = r.getAs[java.math.BigDecimal]("revenue")
  println(f"Year $year: INR ${revenue.doubleValue}%,.2f")
}

System.exit(0)
