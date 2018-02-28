package edu.knoldus

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object operation {

  val scf: SparkConf = new SparkConf().setMaster("local").setAppName("spark-demo")

  val spark: SparkSession = SparkSession.builder().appName("spark-demo").config(scf).getOrCreate()
  val boolean: Boolean = true

  val matchRecordDF: DataFrame = spark.read.option("header", "true").option("inferSchema", boolean).csv("src/main/resources/notes.csv")

  def homeTeam: DataFrame = matchRecordDF.groupBy("HomeTeam").count().toDF("Team", "HomePlay")

  def awayTeam: DataFrame = matchRecordDF.groupBy("AwayTeam").count().toDF("Team", "AwayPlay")

  /**
    * gets top 10 teams with max wining percentage
    * @return
    */

  def topWiningPercentage: DataFrame = {
    val homeRes: DataFrame = matchRecordDF.select("HomeTeam", "FTR").toDF().where("FTR == 'H'").withColumn("count", lit(1)).groupBy("HomeTeam").sum("count").toDF("Team", "HomeSum")
    val awayRes: DataFrame = matchRecordDF.select("AwayTeam", "FTR").toDF().where("FTR == 'A'").withColumn("count", lit(1)).groupBy("AwayTeam").sum("count").toDF("Team", "AwaySum")
    homeRes.join(awayRes, "Team").join(homeAndAwayPlay, "Team").createOrReplaceTempView("winTable")
    spark.sql("select Team,(HomeSum + AwaySum) * 100 /(HomePlay + AwayPlay) as WinPercentage from winTable order by (HomeSum + AwaySum) * 100 /(HomePlay + AwayPlay) desc limit(10)")
  }
  val homeAndAwayPlay: DataFrame = homeTeam.join(awayTeam, "Team")

  import spark.implicits._

  case class MatchRecord(homeTeam: String, awayTeam: String, FTHG: Int, FTAG: Int, FTR: String)

  val matchRecordDataSet: Dataset[MatchRecord] = matchRecordDF.select("HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR").as[MatchRecord]

  /**
    * gets total matches played bya a team
    * @return
    */

  case class TeamAndInfo(team: String,matchesOrWin: Int)

  def getTotalMatch: Dataset[TeamAndInfo] = {
    matchRecordDataSet.groupBy("HomeTeam").count().withColumnRenamed("HomeTeam","Team").withColumnRenamed("count","HomeMatches").join(matchRecordDataSet.groupBy("AwayTeam").count().withColumnRenamed("AwayTeam","Team").withColumnRenamed("count","AwayMatches"), "Team").createOrReplaceTempView("Matches")
    spark.sql("select Team,HomeMatches + AwayMatches as TotalMatches from Matches ").as[TeamAndInfo]
  }


  /**
    * gets top 10 teams with max wins
    * @return
    */
  def getTotalWins: Dataset[TeamAndInfo] = {
    matchRecordDataSet.filter(record => record.FTR == "H").groupBy("HomeTeam").count().toDF("Team", "HomeWin").join(matchRecordDataSet.filter(record => record.FTR == "A").groupBy("AwayTeam").count().toDF("Team", "AwayWin"), "Team").createOrReplaceTempView("winTable")
    spark.sql("select Team,HomeWin + AwayWin as TotalWins from winTable order by HomeWin + AwayWin desc limit(10)").as[TeamAndInfo]

  }




}
