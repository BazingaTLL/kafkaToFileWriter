package com.kafkatofilewriter.app

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger

import com.kafkatofilewriter.app.commonUtils
import com.kafkatofilewriter.app.sparkUtils


object kafkaToFileWriter {
    val logger = Logger.getLogger(this.getClass.getName)

    def main(args: Array[String]) = {
        logger.info("Hello Big Guy!")

        val configMap = commonUtils.readConfFile(args(0))
        val sparkConfig = configMap("sparkConfig")
        val readStreamConfig = configMap("readStreamConfig")
        val writeStreamConfig = configMap("writeStreamConfig")
        val fileConfig = configMap("fileConfig")
        val mognoConfig = configMap("mognoConfig")
        val withWatermarkConfig = configMap("withWatermarkConfig")

        val spark = sparkUtils.getSparkSession(sparkConfig)

        sparkUtils.registerCallbacks(spark, fileConfig)

        val df = sparkUtils.getReadStreamDF(spark, readStreamConfig, withWatermarkConfig)

        val sink = sparkUtils.writeDataToFile(df, writeStreamConfig, fileConfig)

        sink.awaitTermination()

        sparkUtils.endSparkSession(spark)
    }
}