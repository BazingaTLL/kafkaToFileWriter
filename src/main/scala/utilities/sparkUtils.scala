package com.kafkatofilewriter.app

import org.apache.log4j.Logger
import scala.io.Source
import java.io.File
import scala.collection._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryStartedEvent
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryTerminatedEvent
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent


object sparkUtils {
    val logger = Logger.getLogger(this.getClass.getName)

    def getSparkSession(sparkConfig: Map[String, String]) = {
        val appName = if(sparkConfig.contains("appName")) sparkConfig("appName") else "kafkaToFileWriter"
        logger.info(s"app name - $appName")
        val session = SparkSession.builder.appName(appName).getOrCreate()
        session
    }

    def endSparkSession(spark: SparkSession) = {
        spark.stop()
    }

    def getListOfFiles(dir: String, format: String): List[String] = {
        val file = new File(dir)
        file.listFiles.filter(_.isFile)
            .filter(_.getName.startsWith("part-"))
            .filter(_.getName.endsWith(s".$format"))
            .map(_.getPath).toList
    }


    def registerCallbacks(spark: SparkSession, fileConfig: Map[String, String]) = {
        spark.streams.addListener(new StreamingQueryListener() {
            override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
                logger.info("Query started: " + queryStarted.id)
            }
            override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
                logger.info("Query terminated: " + queryTerminated.id)
            }
            override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
                val fileList = this.getListOfFiles(fileConfig("destPath"), fileConfig("format"))
                logger.info()
                logger.info("Query made progress: " + queryProgress.progress)
            }
        })
    }

    def getReadStreamDF(spark: SparkSession, readStreamConfig: Map[String, String],
        withWatermarkConfig: Map[String, String]) = {

        val df = spark.readStream.format("kafka")
        .options(readStreamConfig)
        .load()
        .withWatermark(withWatermarkConfig("withWatermarkCol"), withWatermarkConfig("withWatermarkTm"))

        df
    }

    def getTrigger(timeInSec: String) = {
        val trigger = Trigger.ProcessingTime(s"$timeInSec seconds")
        trigger
    }

    def writeDataToFile(df: DataFrame, writeStreamConfig: Map[String, String],
        fileConfig: Map[String, String]) = {

        val sink = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp")
        .writeStream.queryName(writeStreamConfig("queryName"))
        .foreachBatch{ (outputDf: DataFrame, batchId: Long) =>
            if (!outputDf.isEmpty) { 
                logger.info(s"Processing batch - $batchId")
                outputDf.write.format(fileConfig("format"))
                .mode(fileConfig("mode"))
                .save(fileConfig("destPath"))
            }
        }
        .trigger(this.getTrigger(writeStreamConfig("triggerTimeInSec")))
        .outputMode(writeStreamConfig("outputMode"))
        .start()

        sink
    }

}