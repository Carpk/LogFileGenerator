import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
//import org.apache.hadoop.mapred.*
import org.apache.hadoop.mapreduce.*
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat


import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*


object TypeDistribution {

  class TokenizerMapper extends Mapper[Object,Text,Text,IntWritable]{
    val one = new IntWritable(1)
    val word = new Text

    def map(key: Any, value: Text, context: Mapper[Object,Text,Text,IntWritable]#Context): Unit = {
      for (t <- value.toString().split("\\s")) {
        word.set(t)
        context.write(word, one)
      }
    }
  }

  class IntSumReducer extends Reducer[Text,IntWritable,Text,IntWritable] {
    private val result = new IntWritable
//    override
    def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text,IntWritable,Text,IntWritable]#Context): Unit = {
      val sum = values.foldLeft(0) { (t, i) => t + i.get }
      context.write(key, new IntWritable(sum))
    }
  }

  def main(args: Array[String]): Unit = {
    var conf : Configuration = new Configuration
//    conf.set()
    val input = "log/LogFileGenerator.2022-10-03.log"
    val output = "reports/report.log"

    var job = Job.getInstance(conf, "type distribution")
    job.setJarByClass(classOf[TokenizerMapper])
    job.setMapperClass(classOf[TokenizerMapper])

    job.setCombinerClass(classOf[IntSumReducer])
    job.setReducerClass(classOf[IntSumReducer])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(job, new Path(input)) // args(0)
    FileOutputFormat.setOutputPath(job, new Path(output)) // args(1)

    if (job.waitForCompletion(true)) 0 else 1

    println("TypeDistribution ran successfully")
  }
}






