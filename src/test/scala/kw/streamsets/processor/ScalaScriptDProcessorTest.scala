package kw.streamsets.processor

import java.util

import com.streamsets.pipeline.api.{Field, Record}
import com.streamsets.pipeline.sdk.{ProcessorRunner, RecordCreator, StageRunner}
import kw.streamsets.processor.config.MavenConfig
import org.junit.Assert
import org.scalatest._

import scala.collection.JavaConverters._

class ScalaScriptDProcessorTest extends FlatSpec with BeforeAndAfter with Matchers {
  val mvn = new MavenConfig
  before {
    mvn.remoteRepo = List("http://maven.aliyun.com/nexus/content/groups/public/").asJava
    mvn.artifacts = Nil.asJava
  }

  "scala script" should "compiler" in {
    val processor = new ScalaScriptDProcessor
    processor.mavenConfig = mvn
    processor.scalaScriptBean.initScript =
      """
        |RECORD{  (record,maker) =>
        |   maker.addRecord(record)
        |}
        |destroy{
        |   log.info("end")
        |}
      """.stripMargin
    val runner = new ProcessorRunner.Builder(classOf[ScalaScriptDProcessor], processor)
      .addOutputLane("output")
      .build
    runner.runInit()
    try {
      val record: Record = RecordCreator.create
      record.set(Field.create(true))
      val output: StageRunner.Output = runner.runProcess(util.Arrays.asList(record))
      output.getRecords.asScala.foreach(println)
      Assert.assertEquals(1, output.getRecords.get("output").size)
      Assert.assertEquals(true, output.getRecords.get("output").get(0).get.getValueAsBoolean)
    } finally runner.runDestroy()
  }
}
