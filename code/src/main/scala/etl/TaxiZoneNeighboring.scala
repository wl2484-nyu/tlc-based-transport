package etl

import etl.Utils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

/*
* Manually constructed using the Taxi Zone maps per borough:
* Bronx: https://www.nyc.gov/assets/tlc/images/content/pages/about/taxi_zone_map_bronx.jpg
* Brooklyn: https://www.nyc.gov/assets/tlc/images/content/pages/about/taxi_zone_map_brooklyn.jpg
* Manhattan: https://www.nyc.gov/assets/tlc/images/content/pages/about/taxi_zone_map_manhattan.jpg
* Queens: https://www.nyc.gov/assets/tlc/images/content/pages/about/taxi_zone_map_queens.jpg
* Staten Island: https://www.nyc.gov/assets/tlc/images/content/pages/about/taxi_zone_map_staten_island.jpg
* */
object TaxiZoneNeighboring {
  val EARTH_RADIUS_M = 6378137d // Equatorial radius (WGS84) in meters

  val connected = Map(
    "Bronx" -> Map( // TODO: future work
      3 -> List(),
      18 -> List(),
      20 -> List(),
      31 -> List(),
      32 -> List(),
      47 -> List(),
      51 -> List(),
      58 -> List(),
      59 -> List(),
      60 -> List(),
      69 -> List(),
      78 -> List(),
      81 -> List(),
      94 -> List(),
      119 -> List(),
      126 -> List(),
      136 -> List(),
      147 -> List(),
      159 -> List(),
      167 -> List(),
      168 -> List(),
      169 -> List(),
      174 -> List(),
      182 -> List(),
      183 -> List(),
      184 -> List(),
      185 -> List(),
      212 -> List(),
      213 -> List(),
      200 -> List(),
      208 -> List(),
      220 -> List(),
      235 -> List(),
      240 -> List(),
      241 -> List(),
      242 -> List(),
      247 -> List(),
      248 -> List(),
      250 -> List(),
      254 -> List(),
      259 -> List()
    ),
    "Brooklyn" -> Map( // TODO: future work
      11 -> List(),
      25 -> List(),
      14 -> List(),
      22 -> List(),
      17 -> List(),
      21 -> List(),
      26 -> List(),
      33 -> List(),
      29 -> List(),
      34 -> List(),
      35 -> List(),
      36 -> List(),
      37 -> List(),
      39 -> List(),
      40 -> List(),
      49 -> List(),
      52 -> List(),
      54 -> List(),
      55 -> List(),
      61 -> List(),
      62 -> List(),
      63 -> List(),
      65 -> List(),
      72 -> List(),
      66 -> List(),
      67 -> List(),
      71 -> List(),
      80 -> List(),
      85 -> List(),
      76 -> List(),
      77 -> List(),
      89 -> List(),
      91 -> List(),
      97 -> List(),
      106 -> List(),
      108 -> List(),
      111 -> List(),
      112 -> List(),
      149 -> List(),
      150 -> List(),
      123 -> List(),
      133 -> List(),
      154 -> List(),
      155 -> List(),
      165 -> List(),
      178 -> List(),
      177 -> List(),
      181 -> List(),
      189 -> List(),
      190 -> List(),
      188 -> List(),
      195 -> List(),
      210 -> List(),
      217 -> List(),
      225 -> List(),
      222 -> List(),
      227 -> List(),
      228 -> List(),
      257 -> List(),
      255 -> List(),
      256 -> List()
    ),
    "EWR" -> Map(),
    "Manhattan" -> Map(
      4 -> List(224, 232, 79),
      24 -> List(166, 41, 43, 151),
      12 -> List(13, 261, 88),
      13 -> List(231, 261, 12),
      41 -> List(42, 74, 75, 43, 24, 166),
      45 -> List(144, 148, 232, 209, 231),
      42 -> List(120, 74, 41, 152, 116),
      43 -> List(41, 75, 236, 237, 163, 142, 239, 238, 151, 24),
      48 -> List(142, 163, 230, 100, 68, 246, 50),
      50 -> List(143, 48, 246),
      68 -> List(246, 48, 100, 186, 90, 249, 158),
      79 -> List(107, 224, 4, 148, 114, 113),
      74 -> List(42, 75, 41),
      75 -> List(41, 74, 262, 263, 236, 43),
      87 -> List(261, 209, 88),
      88 -> List(261, 87, 12),
      90 -> List(68, 186, 234, 249),
      125 -> List(158, 249, 114, 211, 231),
      100 -> List(48, 230, 164, 186, 68),
      107 -> List(234, 170, 137, 224, 79),
      113 -> List(234, 79, 114, 249),
      114 -> List(249, 113, 79, 144, 211, 125),
      116 -> List(244, 42, 152),
      120 -> List(243, 127, 42, 244),
      127 -> List(128, 120, 243),
      128 -> List(127, 243),
      151 -> List(24, 43, 238),
      140 -> List(262, 229, 141),
      137 -> List(170, 233, 224, 107),
      141 -> List(236, 263, 140, 229, 237),
      142 -> List(239, 43, 163, 48, 143),
      152 -> List(116, 42, 166),
      143 -> List(239, 142, 50),
      144 -> List(114, 148, 45, 231, 211),
      148 -> List(79, 232, 45, 144),
      158 -> List(246, 68, 249, 125),
      161 -> List(163, 162, 170, 164, 230),
      162 -> List(237, 229, 233, 170, 161, 163),
      163 -> List(142, 43, 237, 162, 161, 230, 48),
      164 -> List(161, 170, 234, 186, 100),
      170 -> List(161, 162, 233, 137, 107, 164),
      166 -> List(152, 41, 24),
      186 -> List(68, 100, 164, 234, 90),
      209 -> List(231, 45, 87, 261),
      211 -> List(114, 144, 231, 125),
      224 -> List(107, 137, 4, 79),
      229 -> List(141, 140, 233, 162),
      230 -> List(48, 163, 161, 100),
      231 -> List(125, 211, 144, 45, 209, 261, 13),
      239 -> List(238, 43, 142, 143),
      232 -> List(148, 4, 45),
      233 -> List(162, 229, 137, 170),
      234 -> List(186, 164, 107, 113, 90),
      236 -> List(43, 75, 263, 141, 237),
      237 -> List(43, 236, 141, 162, 163),
      238 -> List(151, 43, 239),
      263 -> List(75, 262, 141, 236),
      243 -> List(128, 127, 120, 244),
      244 -> List(243, 120, 116),
      246 -> List(50, 48, 68, 158),
      249 -> List(68, 90, 113, 114, 125, 158),
      261 -> List(13, 231, 209, 87, 88, 12),
      262 -> List(75, 140, 263)
    ),
    "Queens" -> Map( // TODO: future work
      7 -> List(),
      8 -> List(),
      9 -> List(),
      10 -> List(),
      15 -> List(),
      16 -> List(),
      19 -> List(),
      28 -> List(),
      38 -> List(),
      53 -> List(),
      56 -> List(),
      56 -> List(),
      64 -> List(),
      73 -> List(),
      70 -> List(),
      82 -> List(),
      83 -> List(),
      92 -> List(),
      93 -> List(),
      95 -> List(),
      96 -> List(),
      98 -> List(),
      101 -> List(),
      102 -> List(),
      121 -> List(),
      122 -> List(),
      124 -> List(),
      129 -> List(),
      134 -> List(),
      130 -> List(),
      139 -> List(),
      131 -> List(),
      132 -> List(),
      135 -> List(),
      138 -> List(),
      145 -> List(),
      146 -> List(),
      157 -> List(),
      160 -> List(),
      171 -> List(),
      173 -> List(),
      175 -> List(),
      179 -> List(),
      180 -> List(),
      191 -> List(),
      192 -> List(),
      193 -> List(),
      196 -> List(),
      203 -> List(),
      197 -> List(),
      198 -> List(),
      205 -> List(),
      207 -> List(),
      215 -> List(),
      216 -> List(),
      218 -> List(),
      219 -> List(),
      226 -> List(),
      223 -> List(),
      258 -> List(),
      253 -> List(),
      252 -> List(),
      260 -> List()
    ),
    "Staten Island" -> Map( // TODO: future work
      5 -> List(),
      6 -> List(),
      23 -> List(),
      44 -> List(),
      84 -> List(),
      99 -> List(),
      109 -> List(),
      110 -> List(),
      115 -> List(),
      118 -> List(),
      156 -> List(),
      172 -> List(),
      176 -> List(),
      187 -> List(),
      204 -> List(),
      206 -> List(),
      214 -> List(),
      221 -> List(),
      245 -> List(),
      251 -> List()
    )
  ).mapValues(_.map { case (k, v) => k.toLong -> v.map(_.toLong) })

  val isolated = Map(
    "Bronx" -> List(46, 199),
    "Brooklyn" -> List(),
    "EWR" -> List(1),
    "Manhattan" -> List(103, 104, 105, 153, 194, 202),
    "Queens" -> List(2, 27, 30, 86, 117, 201),
    "Staten Island" -> List()
  ).mapValues(_.map(_.toLong))

  def calcGeoDistanceInMeter(startLat: Double, startLon: Double, endLat: Double, endLon: Double): Int = {
    val latDiff = math.toRadians(endLat - startLat)
    val lonDiff = math.toRadians(endLon - startLon)
    val lat1 = math.toRadians(startLat)
    val lat2 = math.toRadians(endLat)

    val a = math.sin(latDiff / 2) * math.sin(latDiff / 2) +
      math.sin(lonDiff / 2) * math.sin(lonDiff / 2) * math.cos(lat1) * math.cos(lat2)
    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    (EARTH_RADIUS_M * c).toInt
  }

  def calcGeoDistanceInKilometer(startLat: Double, startLon: Double, endLat: Double, endLon: Double): Double = {
    calcGeoDistanceInMeter(startLat, startLon, endLat, endLon) / 1000.0
  }

  def getBoroughConnectedLocationMap(borough: String = "Manhattan"): Map[Long, List[Long]] = connected(borough)

  def getBoroughIsolatedLocationList(borough: String = "Manhattan"): List[Long] = isolated(borough)

  def saveConnectedLocations(spark: SparkSession, path: String): Unit = {
    import spark.implicits._

    connected.mapValues(_.keys.mkString(","))
      .toSeq
      .toDF("borough", "location_ids")
      .orderBy(asc("borough"))
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite) // workaround for abnormal path-already-exists error
      .option("header", true)
      .option("delimiter", "\t")
      .option("emptyValue", null) // make sure a column with empty value would not be quoted by double quotes
      .csv(f"$path/borough_connected_locations")
  }

  def saveIsolatedLocations(spark: SparkSession, path: String): Unit = {
    import spark.implicits._

    isolated.mapValues(_.mkString(","))
      .toSeq
      .toDF("borough", "location_ids")
      .orderBy(asc("borough"))
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite) // workaround for abnormal path-already-exists error
      .option("header", true)
      .option("delimiter", "\t")
      .option("emptyValue", null) // make sure a column with empty value would not be quoted by double quotes
      .csv(f"$path/borough_isolated_locations")
  }

  def main(args: Array[String]): Unit = {
    val options = parseOpts(Map(), args.toList)
    val intermediateOutputPath = options(keyIntermediateOutput).asInstanceOf[String]

    val spark = SparkSession.builder().appName("TaxiZoneNeighboringETL").getOrCreate()

    // save intermediate data
    saveConnectedLocations(spark, intermediateOutputPath)
    saveIsolatedLocations(spark, intermediateOutputPath)
  }
}
