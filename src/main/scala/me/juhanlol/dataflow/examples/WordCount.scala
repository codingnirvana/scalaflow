package me.juhanlol.dataflow.examples

import com.google.cloud.dataflow.sdk.options._
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath
import me.juhanlol.dataflow.DataPipeline


object WordCount extends App {

  class OutputFactory extends DefaultValueFactory[String] {
    override def create(options: PipelineOptions): String = {
      val dataflowOptions = options.as(classOf[DataflowPipelineOptions])
      if (dataflowOptions.getStagingLocation != null) {
        GcsPath.fromUri(dataflowOptions.getStagingLocation)
          .resolve("counts.txt").toString
      } else {
        throw new IllegalArgumentException("Must specify --output or --stagingLocation")
      }
    }
  }

  trait WordCountOptions extends ApplicationNameOptions {
    @Description("Path of the file to read from")
    @Default.String("gs://dataflow-samples/shakespeare/kinglear.txt")
    def getInput: String
    def setInput(value: String)

    @Description("Path of the file to write to")
    @Default.InstanceFactory(classOf[OutputFactory])
    def getOutput: String
    def setOutput(value: String)
  }


  val options = PipelineOptionsFactory
    .fromArgs(args)
    .withValidation()
    .as(classOf[WordCountOptions])

  options.setAppName(this.getClass.getSimpleName + "-scala")

  val pipeline = new DataPipeline(options)

  pipeline
    .text(options.getInput)
    .flatMap(line => line.split("[^a-zA-Z']+"))
    .countPerElement()
    .map { count => count.getKey + "\t" + count.getValue.toString }
    .save(options.getOutput, Some("writeCounts"))

  pipeline.run
}