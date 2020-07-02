package com.broadtech.sdc.common

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import scala.collection.mutable.ArrayBuffer

/**
 *  hdfs 文件工具类
 */
object HadoopFileUtil {
  def getHdfs(path: String) = {
    val conf = new Configuration()
    FileSystem.get(URI.create(path), conf)
  }

  def getFilesAndDirs(path: String): Array[Path] = {
    val fs = getHdfs(path).listStatus(new Path(path))
    FileUtil.stat2Paths(fs)
  }

  /**
   * 打印所有的文件名，包括子目录
   */
  def listAllFiles(path: String, separator: String): String = {
    val fileList = ArrayBuffer[String]()
    val hdfs = getHdfs(path)
    val listPath = getFilesAndDirs(path)
    listPath.foreach(path => {
      if (hdfs.getFileStatus(path).isFile())
        fileList.+=(path.toString)
      else {
        listAllFiles(path.toString(), separator)

      }
    })

    fileList.mkString(separator)
  }

  /**
   * 打印一级文件名
   */
  def listFiles(path: String) {
    getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isFile()).foreach(println)
  }

  /**
   * 打印一级目录名
   */
  def listDirs(path: String) {
    getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isDirectory()).foreach(println)
  }

  /**
   * 打印一级文件名和目录名
   */
  def listFilesAndDirs(path: String) {
    getFilesAndDirs(path).foreach(println)
  }


  /** ************返回数组 ************/
  def getAllFiles(path: String): ArrayBuffer[Path] = {
    val arr = ArrayBuffer[Path]()
    val hdfs = getHdfs(path)
    val listPath = getFilesAndDirs(path)
    listPath.foreach(path => {
      if (hdfs.getFileStatus(path).isFile()) {
        arr += path
      } else {
        arr ++= getAllFiles(path.toString())
      }

    })
    arr
  }

  def getFiles(path: String): Array[Path] = {
    getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isFile())
  }

  def getDirs(path: String): Array[Path] = {
    getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isDirectory())
  }
}
