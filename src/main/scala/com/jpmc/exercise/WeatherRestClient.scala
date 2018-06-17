package com.jpmc.exercise

import java.io.IOException
import java.net.SocketTimeoutException

import scala.io.Source

/**
  * Created by adnan_saqib on 16/06/2018.
  */
object WeatherRestClient {
  
  def get(url: String,
          connectTimeout: Int = 5000,
          readTimeout: Int = 5000,
          requestMethod: String = "GET") =
  {
    import java.net.{HttpURLConnection, URL}
    try {
      val connection = (new URL(url)).openConnection.asInstanceOf[HttpURLConnection]
      connection.setConnectTimeout(connectTimeout)
      connection.setReadTimeout(readTimeout)
      connection.setRequestMethod(requestMethod)
      val inputStream = connection.getInputStream
      val content = managed(inputStream) { inputStream => {
        Source.fromInputStream(inputStream).getLines().toList
      }}
      content
    } catch {
      case ioe: IOException =>  List("Station Data Cannot be Extracted")
      case ste: SocketTimeoutException => List("Station Data Cannot be Extracted")
    }
  }

  def managed[A <: {def close() : Unit}, B](resource: A)(f: A => B): B = {
    try {
      f(resource)
    } finally {
      resource.close()
    }
  }
}
