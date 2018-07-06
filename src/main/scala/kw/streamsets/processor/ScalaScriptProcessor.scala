package kw.streamsets.processor

import java.net.{URL, URLClassLoader}
import java.util
import java.util.Date

import com.streamsets.pipeline.api._
import com.streamsets.pipeline.api.base.SingleLaneProcessor.{SingleLaneBatchMaker => Maker}
import com.streamsets.pipeline.api.base.{OnRecordErrorException, SingleLaneProcessor}
import kw.common.{LogWriter, Logging}
import kw.common.data.DependencyUtils
import kw.common.data.DependencyUtils.MavenCoordinate
import kw.streamsets.common.{DefaultErrorRecordHandler, ErrorRecordHandler}
import kw.streamsets.common.implicits.Decorators
import org.slf4j.Logger

import scala.collection.JavaConverters._
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.IR
import scala.tools.nsc.interpreter.{IMain, JList, JPrintWriter}


abstract class ScalaScriptProcessor
  extends SingleLaneProcessor
    with Logging {
  self =>

  val scriptConfigGroup = "ScalaScript"
  val scriptingEngineName = "Scala"
  val exclusions: Seq[String] = Seq.empty

  var errorRecordHandler: ErrorRecordHandler = _
  var callBack: ProcessCallBack = _
  var interp: IMain = _

  /**
    * sdc常用功能
    */
  object SdcFunction extends Decorators {
    val log: Logger = self.log

    def context: Processor.Context = self.getContext

    def err(record: Record, message: String): Unit = {
      errorRecordHandler.onError(
        new OnRecordErrorException(
          record,
          Errors.SCRIPTING_07,
          message
        )
      )
    }
  }

  def scripts: String

  def packages: JList[MavenCoordinate]

  def remoteRepos: JList[String]

  def createIMain(jars: Seq[String], out: JPrintWriter, classLoader: ClassLoader): IMain = {
    val setting = new Settings()
    setting.usejavacp.value = true
    setting.embeddedDefaults(classLoader)
    jars
      .foreach(url => {
        setting.classpath.append(url)
        log.info("add jar:" + url)
      })
    urlsParser("", classLoader, Nil).foreach {
      url =>
        setting.classpath.append(url.toString)
        log.info("add jar:" + url)
    }
    val imain = new IMain(setting, out)
    imain.settings.embeddedDefaults(classLoader)
    imain
  }

  def urlsParser(name: String, x: ClassLoader, result: List[URL]): List[URL] = {
    val thisName = name + "/" + x.getClass.getName
    log.info(thisName)
    val (next, res) = x match {
      case urls: URLClassLoader =>
        val r0 = urls.getURLs.foldLeft(result)((a, b) => b :: a)
        val n0 = urls.getParent
        (n0, r0)
      case other =>
        val n0 = other.getParent
        (n0, result)
    }
    if (next != null) {
      urlsParser(thisName, next, res)
    } else {
      res.reverse
    }
  }

  @SuppressWarnings(Array("deprecation"))
  override def init(): util.List[Stage.ConfigIssue] = {
    val issues = super.init()
    if (scripts == null || scripts.trim.isEmpty) {
      issues.add(getContext.createConfigIssue(scriptConfigGroup, "scripts", Errors.SCRIPTING_02))
    } else {

      errorRecordHandler = new DefaultErrorRecordHandler(getContext)
      callBack = new ProcessCallBack
      val scriptOut = new JPrintWriter(new LogWriter(log))
      try {
        val jars = DependencyUtils.resolveMavenDependencies(exclusions, packages.asScala, remoteRepos.asScala, "", None)
        val cl1 = Thread.currentThread().getContextClassLoader
        interp = createIMain(jars, scriptOut, cl1)
        if (interp == null) issues.add(getContext.createConfigIssue(null, null, Errors.SCRIPTING_00, scriptingEngineName))
      } catch {
        case ex: Throwable =>
          issues.add(
            getContext.createConfigIssue(null, null, Errors.SCRIPTING_01, scriptingEngineName, ex.toString, ex)
          )
          log.error(ex.getMessage, ex)
      }

      interp.bind("callBack", callBack)
      interp.bind("self", self)
      val initScript =
        s"""
           |import callBack._
           |import self.SdcFunction._
           |import com.streamsets.pipeline.api._
           |import com.streamsets.pipeline.api.base.SingleLaneProcessor._
          """.stripMargin
      //        log.info("init script:" + initScript)
      //      log.info("user script:\n" + scripts)
      interp.interpret(initScript)
      interp.interpret(scripts) match {
        case IR.Success =>
          if (!callBack.isValid) {
            issues.add(getContext.createConfigIssue(scriptConfigGroup, "script", Errors.SCRIPTING_04))
            log.error(Errors.SCRIPTING_04.getMessage)
          }
        case IR.Error =>
          issues.add(getContext.createConfigIssue(scriptConfigGroup, "script", Errors.SCRIPTING_03))
          log.error(Errors.SCRIPTING_03.getMessage)
        case _ =>
      }

    }
    issues
  }

  override def process(batch: Batch, maker: Maker): Unit = {
    val it = batch.getRecords.asScala
    if (callBack.isBatch) try callBack.process(it, maker) catch {
      case ex: Exception =>
        throw new StageException(Errors.SCRIPTING_06, ex.toString, ex)
    } else it.foreach { record =>
      try callBack.process(record, maker) catch {
        case ex: Exception =>
          errorRecordHandler.onError(
            new OnRecordErrorException(
              record,
              Errors.SCRIPTING_05,
              ex.toString,
              ex
            )
          )
      }
    }
  }

  override def destroy(): Unit = {
    try {
      callBack.destroy()
      interp.close()
    } catch {
      case e: Exception =>
        log.error(Errors.SCRIPTING_09.getMessage, e)
    }
    super.destroy()
  }
}
