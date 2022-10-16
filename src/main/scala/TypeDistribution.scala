import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*

import java.io.{File, IOException}
import java.util
import scala.jdk.CollectionConverters.*


class TypeDistribution
object TypeDistribution:
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      import HelperUtils.Parameters.*
      var blockTime = 0

      // number of characters in each log message for each log message type that contain the
      // highest number of characters in the detected instances of the designated regex pattern
      value.toString.split("\\n").foreach(v =>
        val lineArr = v.split("\\s+")
        val logTime = lineArr(0).substring(0,2).toInt * 60 + lineArr(0).substring(6,8).toInt

        if (logTime > blockTime) {
          blockTime = logTime + intervalTime

        }

        if (infoTag.r.findAllIn(v).nonEmpty) {
          output.collect(new Text(infoTag), new IntWritable(lineArr(5).length))
        } else if (warnTag.r.findAllIn(v).nonEmpty) {
          output.collect(new Text(warnTag), new IntWritable(lineArr(5).length))
        } else if (debugTag.r.findAllIn(v).nonEmpty) {
          output.collect(new Text(debugTag), new IntWritable(lineArr(5).length))
        } else if (errorTag.r.findAllIn(v).nonEmpty) {
          output.collect(new Text(errorTag), new IntWritable(lineArr(5).length))
        }
      )



  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key,  new IntWritable(sum.get()))


  def main(args: Array[String]): Unit =
    import HelperUtils.Parameters.*
    val conf: JobConf = new JobConf(this.getClass)

    conf.setJobName(typeDistJob)
    //    conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", numMapJobs)
    conf.set("mapreduce.job.reduces", numRedJobs)

    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])

    conf.setMapperClass(classOf[Map])

    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])

    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])

    FileInputFormat.setInputPaths(conf, new Path(args(0)))
    FileOutputFormat.setOutputPath(conf, new Path(args(1) + "/" + charCountJob))

    JobClient.runJob(conf)

