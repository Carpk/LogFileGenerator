import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*

import java.io.{File, IOException}
import java.util
import scala.jdk.CollectionConverters.*



object TimeIntervalSort:
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1)
    private val txt = new Text()

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val info = "INFO"
      val warn = "WARN"
      val debug = "DEBUG"
      val error = "ERROR"

      // Then, for each message type you will produce the number of the generated log messages.
      for (v <- value.toString.split("\\n")) { // no for loops
        if (info.r.findAllIn(v).nonEmpty) {
          txt.set("info: ")
        } else if (warn.r.findAllIn(v).nonEmpty) { // change to const
          txt.set("warn: ")
        } else if (debug.r.findAllIn(v).nonEmpty) {
          txt.set("debug: ")
        } else if (error.r.findAllIn(v).nonEmpty) {
          txt.set("error: ")
        }

        output.collect(txt, one)
      }


  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key,  new IntWritable(sum.get()))


  def main(args: Array[String]): Unit =
    val conf: JobConf = new JobConf(this.getClass)
    val input = "log/LogFileGenerator.2022-09-22.log"
    val output = "reports/type_count"

    //    val dir = new Directory(new File("/path"))
    //    dir.deleteRecursively()

    //    val fs = FileSystem.get()
    //    val outPutPath = new Path(output)
    //    fs.delete(outPutPath, true)

    conf.setJobName("TypeCount")
    //    conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "5")
    conf.set("mapreduce.job.reduces", "2")

    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])

    conf.setMapperClass(classOf[Map])

    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])

    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])

    FileInputFormat.setInputPaths(conf, new Path(input))
    FileOutputFormat.setOutputPath(conf, new Path(output))
    JobClient.runJob(conf)


