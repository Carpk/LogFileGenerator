import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
//import org.apache.hadoop.mapred.*
import org.apache.hadoop.mapreduce.*
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import java.util.StringTokenizer


import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*


object TypeDistribution {

  class TokenizerMapper extends Mapper[Object,Text,Text,IntWritable]{
    val one = new IntWritable(1)
    val word = new Text

    def map(key: Any, value: Text, context: Mapper[Object,Text,Text,IntWritable]#Context): Unit = {
      for (t <- value.toString().split("\\s")) {
        println("PRINTLN: " + t)
//        word.set(t)
//        context.write(word, one)
      }

      val itr = new StringTokenizer(value.toString)
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken())
        context.write(word, one)
      }
    }
  }

  class IntSumReducer extends Reducer[Text,IntWritable,Text,IntWritable] {
    val result = new IntWritable

    def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text,IntWritable,Text,IntWritable]#Context): Unit = {
      val sum = values.foldLeft(0) { (t, i) => t + i.get }
      context.write(key, new IntWritable(sum))
    }
  }

  def main(args: Array[String]): Unit = {
    var conf : Configuration = new Configuration
    
    val input = "log/LogFileGenerator.2022-10-03.log"
    val output = "reports/type_dist"

    var job = Job.getInstance(conf, "type distribution")
    job.setJarByClass(classOf[TokenizerMapper])
    job.setMapperClass(classOf[TokenizerMapper])

    job.setCombinerClass(classOf[IntSumReducer])
    job.setReducerClass(classOf[IntSumReducer])

    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    FileInputFormat.addInputPath(job, new Path(input)) // args(0)
    FileOutputFormat.setOutputPath(job, new Path(output)) // args(1)

    System.exit(if(job.waitForCompletion(true))  0 else 1)
  }
}






