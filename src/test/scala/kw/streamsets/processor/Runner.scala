package kw.streamsets.processor

import java.util

import com.streamsets.pipeline.api.{Field, Record}
import com.streamsets.pipeline.sdk.{ProcessorRunner, RecordCreator, StageRunner}

import scala.collection.JavaConverters._

object Runner {

  def main(args: Array[String]): Unit = {
    val processor = new ScalaScriptDProcessor
    processor.scalaScriptBean.initScript = "RECORD{(record,maker) => }"
    val runner = new ProcessorRunner.Builder(classOf[ScalaScriptDProcessor], processor)
      .addOutputLane("output")
      .build
    runner.runInit()
    script(processor)
    val record: Record = RecordCreator.create
    record.set(Field.create(true))
    val output: StageRunner.Output = runner.runProcess(util.Arrays.asList(record))
    output.getRecords.asScala.foreach(println)
  }

  def script(self: ScalaScriptProcessor): Unit = {
    val callBack: ProcessCallBack = self.callBack
    import callBack._
    import self.SdcFunction._
    RECORD {
      (record, maker) =>
        record.set(
          Map(
            "a" -> true.asField,
            "b" -> "hello world",
            "c" -> Map("d" -> "ccc"),
            "e" -> Map.empty
          ).asField
        )
        record.get("/a").getValue.toString
        record.get()
        println(record.get("/c/d").getValueAsString)
        maker.addRecord(record)
    }
    destroy {
      println("end pipeline")
    }
  }

  val Alice = new {
    val flag = true

    def !?(a: Any, b: Any): Any = {
      if (flag) a else b
    }
  }

  def test[T] = (Alice !? (100, "Hello")) match {
    case Some(i: Int) => println("Int received" + i)
    case Some(s: String) => println("String received" + s)
    case _ =>
  }


}
