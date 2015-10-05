package me.juhanlol.dataflow.examples

import java.io.File
import java.lang.Long
import java.net.URI

import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.options._
import com.google.cloud.dataflow.sdk.transforms._
import com.google.cloud.dataflow.sdk.values.{PCollectionList, KV, PCollection, PInput}
import me.juhanlol.dataflow.{DList, DataPipeline}

class TFIDF extends App {

  trait Options extends PipelineOptions {
    @Description("Path to the directory or prefix containing files to read from")
    @Default.String("gs://dataflow-samples/shakespeare/")
    def getInput: String
    def setInput(value: String)

    @Description("Prefix of output URI to write to")
    @Validation.Required
    def getOutput: String
    def setOutput(value: String)
  }


  val options = PipelineOptionsFactory
    .fromArgs(args)
    .withValidation()
    .as(classOf[Options])

  val pipeline = new DataPipeline(options)
  val lines = pipeline.filesToLines(options.getInput)
  //TODO: Register Coder

  val totalDocuments = lines
    .keys()
    .removeDuplicates()
    .countGlobally()
    // TODO: Create Singleton

  val uriToWords: DList[KV[URI, String]] = lines
    .flatMap(kv => {
      val uri = kv.getKey
      val line = kv.getValue
      val words = line.split("\\W+").filter(!_.isEmpty).map(_.toLowerCase)
      words.map(w => KV.of(uri,w))
    })

  val wordToDocCount: DList[KV[String, Long]] = uriToWords
    .removeDuplicates()
    .values()
    .countPerElement()

  val uriToWordCount: DList[KV[URI, Long]] = uriToWords
    .keys()
    .countPerElement()

  val uriAndWordToCount = uriToWords
    .countPerElement()



}
