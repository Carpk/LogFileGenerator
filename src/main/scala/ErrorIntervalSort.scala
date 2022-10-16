import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*

import java.io.{File, IOException}
import java.util
import scala.jdk.CollectionConverters.*


class ErrorIntervalSort
object ErrorIntervalSort:
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    var blockTime = 0
    var timeBlock = ""

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      import HelperUtils.Parameters.*

      // time intervals sorted in the descending order that contained most log messages of the type ERROR
      value.toString.split("\\n").foreach( v=>{
        val s = v.split("\\s+")(0)
        val logTime = (s.substring(0,2).toInt * 3600) + (s.substring(3,5).toInt * 60) + (s.substring(6,8).toInt)

        if (logTime > blockTime) {
          blockTime = logTime + (intervalTime * 60)
          timeBlock = s.substring(0, 12) + intervalTime + "min block"
        }

        if (errorTag.r.findAllIn(v).nonEmpty) {
          output.collect(new Text(timeBlock), new IntWritable(1))
        }
      })


  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key,  new IntWritable(sum.get()))


  def main(args: Array[String]): Unit =
    import HelperUtils.Parameters.*
    val conf: JobConf = new JobConf(this.getClass)

    conf.setJobName(errorCountJob)
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
    FileOutputFormat.setOutputPath(conf, new Path(outDir + "/" + errorCountJob))
    JobClient.runJob(conf)


