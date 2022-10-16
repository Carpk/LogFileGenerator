import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*
//import HelperUtils.{CreateLogger, Parameters}

import java.io.{File, IOException}
import java.util
import scala.jdk.CollectionConverters.*

class TypeCount
object TypeCount:
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      import HelperUtils.Parameters.*

      // each message type you will produce the number of the generated log messages.
      value.toString.split("\\n").foreach(v => {
        if (infoTag.r.findAllIn(v).nonEmpty) {
          output.collect(new Text(infoTag), new IntWritable(1))
        } else if (warnTag.r.findAllIn(v).nonEmpty) {
          output.collect(new Text(warnTag), new IntWritable(1))
        } else if (debugTag.r.findAllIn(v).nonEmpty) {
          output.collect(new Text(debugTag), new IntWritable(1))
        } else if (errorTag.r.findAllIn(v).nonEmpty) {
          output.collect(new Text(errorTag), new IntWritable(1))
        }
      })


  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key,  new IntWritable(sum.get()))


  def main(args: Array[String]): Unit =
    import HelperUtils.Parameters.*
    val conf: JobConf = new JobConf(this.getClass)

    conf.setJobName(typeCountJob)
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

    FileInputFormat.setInputPaths(conf, new Path(inputFile))
    FileOutputFormat.setOutputPath(conf, new Path(outDir + "/" + typeCountJob))
    JobClient.runJob(conf)


