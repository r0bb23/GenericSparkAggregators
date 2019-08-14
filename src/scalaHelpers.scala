package com.ionic.helperfunctions

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }
import java.security.MessageDigest
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._

object ScalaHelpers {
  val logger = LoggerFactory.getLogger(this.getClass.getName)

  def objectToByteArray[Type](obj: Type): Array[Byte] = {
    val byteArrayStream = new ByteArrayOutputStream()
    val outputObj = new ObjectOutputStream(byteArrayStream)
    outputObj.writeObject(obj)
    return byteArrayStream.toByteArray()
  }

  def byteArrayToObject[Type](bytes: Array[Byte]): Type = {
    val byteArrayStream = new ByteArrayInputStream(bytes)
    val inputObj = new ObjectInputStream(byteArrayStream)
    return inputObj.readObject().asInstanceOf[Type]
  }

  def toListString(listAny: List[Any]): List[String] = {
    listAny.map(_.toString).toList
  }
}
