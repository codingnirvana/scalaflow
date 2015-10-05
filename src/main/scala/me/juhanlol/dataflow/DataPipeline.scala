package me.juhanlol.dataflow

import java.io.File
import java.net.URI

import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.options.PipelineOptions
import com.google.cloud.dataflow.sdk.transforms.{PTransform, Flatten, WithKeys, Create}
import com.google.cloud.dataflow.sdk.values.{PCollection, PInput, KV, PCollectionList}

import scala.collection.JavaConversions
import scala.reflect.runtime.universe._


/**
 * Context of a dataflow job
 * It handles coder registration and provide methods for creating the initial
 * DList from existing data source
 */
class DataPipeline(val pipeline: Pipeline) {
  val coderRegistry = new CoderRegistry

  def this(options: PipelineOptions) = {
    this(Pipeline.create(options))
  }

  def run = pipeline.run

  /**
   * Create a DList from a data source
   * Only text data is supported for the moment
   */
  def text(path: String): DList[String] = {
    new DList(pipeline.apply(
      TextIO.Read.named("TextFrom %s".format(path)).from(path)),
      coderRegistry)
  }

 def filesToLines(path: String): DList[KV[URI,String]] = {
    new DList(pipeline.apply(new ReadDocuments(path)), coderRegistry)
 }


  class ReadDocuments(path: String) extends PTransform[PInput, PCollection[KV[URI, String]]] {

    override def apply(input: PInput) = {
      val pipeline = input.getPipeline

      var urisToLines: PCollectionList[KV[URI,String]] = PCollectionList.empty(pipeline)

      val uris = listInputFiles(path)

      uris.foreach {
        uri =>
          val uriString = uri.getScheme match {
            case "file" => new File(uri).getPath
            case _ => uri.toString
          }

          val oneUriToLines: PCollection[KV[URI, String]] = pipeline
            .apply(TextIO.Read.from(uriString).named("TextIO.Read(" + uriString + ")"))
            .apply("WithKeys(" + uriString + ")", WithKeys.of(uri))

          urisToLines = urisToLines.and(oneUriToLines)
      }

      urisToLines.apply(Flatten.pCollections())

    }

    private def listInputFiles(path: String): Set[URI] = {
      val baseUri = new URI(path)
      val absoluteUri = Option(baseUri.getScheme) match {
        case None => baseUri
        case _ => new URI(
          "file",
          baseUri.getAuthority,
          baseUri.getPath,
          baseUri.getQuery,
          baseUri.getFragment
        )
      }
      Option(absoluteUri.getScheme) match {
        case Some("file") =>
          val directory = new File(absoluteUri)
          directory.listFiles().map(_.toURI).toSet
        case _ => throw new UnsupportedOperationException("Only file URI is supported.")
      }

    }

  }










  /**
   * Create a DList from an in-memory collection, usually for test purpose
   */
  def of[T: TypeTag](iter: Iterable[T]): DList[T] = {
    val coder = coderRegistry.getDefaultCoder[T]
    val pcollection = pipeline.apply(
      Math.random().toString, Create.of(JavaConversions.asJavaIterable(iter))).setCoder(coder)
    new DList(pcollection, coderRegistry)
  }
}