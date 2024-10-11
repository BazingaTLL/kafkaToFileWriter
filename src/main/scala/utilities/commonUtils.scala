package com.kafkatofilewriter.app

import org.apache.log4j.Logger
import scala.io.Source
import scala.collection._


object commonUtils {
    val logger = Logger.getLogger(this.getClass.getName)

    def readConfFile(filePath: String) = {
        logger.info(s"Reading file from path - $filePath")

        val bufferSource = Source.fromFile(filePath)
        val text = bufferSource.mkString
        bufferSource.close()

        logger.info(s"data fetched\n $text \n")
        val jsonData = ujson.read(text)

        val returnData = jsonData.obj.asInstanceOf[Map[String, ujson.Obj]]

        var configMap = mutable.Map[String, mutable.Map[String, String]]()

        for (pair <- returnData) {
            var temp = mutable.Map[String, String]()
            for (insidePair <- pair._2.obj.asInstanceOf[Map[String, ujson.Str]]) {
                temp += (insidePair._1 -> insidePair._2.str)
            }
            configMap += (pair._1 -> temp)
        }

        configMap
    }

}