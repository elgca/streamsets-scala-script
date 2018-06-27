package kw.streamsets.processor

import com.streamsets.pipeline.api.Record
import com.streamsets.pipeline.sdk.{ProcessorRunner, StageRunner}
import kw.streamsets.common.DefaultErrorRecordHandler
import kw.streamsets.common.implicits.Decorators

import scala.collection.JavaConverters._

trait ImportScript extends Decorators {
  val processor = new ScalaScriptDProcessor
  val runner = {
    processor.scalaScriptBean.initScript = "RECORD{(record,maker) => }"
    val runner = new ProcessorRunner.Builder(classOf[ScalaScriptDProcessor], processor)
      .addOutputLane("output")
      .build
    processor.callBack = Implicit
    runner
  }

  object Implicit extends ProcessCallBack {
    val self = processor
    val log = processor.SdcFunction.log

    def context = processor.SdcFunction.context

    def err(record: Record, message: String) = processor.SdcFunction.err(record, message)
  }

  def before(f: ScalaScriptDProcessor => Unit): Unit = {
    f(processor)
  }

  def run(records:List[Record]) = {
    runner.runProcess(records.asJava).getRecords.get("output").asScala
  }

}
