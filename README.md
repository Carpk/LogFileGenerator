
# Log File Generator

This is a application that utilizes the Hadoop framework and AWS EMR service to distribute tasks
across multiple nodes for an exercise in distributed computing

### Requirements

This application was built running Hadoop 3.3.0, and Java 15.0.1

Hadoop was installed using this tutorial:
[https://brain-mentors.com/hadoopinstallation/](https://brain-mentors.com/hadoopinstallation/)

## Running the application

Download and install the above-mentioned requirements or deploy directly to AWS

To clean the generated files and build a single fat JAR file: 

`sbt clean assembly`

The run the JAR file with a prompt of which task to execute:

`sbt run`

or run a task directly as described below.

#### Type Distribution Job

Shows the distribution of different types of messages across predefined time intervals

`sbt "runMain TypeDistribution"`

#### Error Interval Sort Job

Time intervals sorted in the descending order that contained most log messages of the type ERROR

`sbt "runMain ErrorIntervalSort"`

#### Type Count Job

Produces the number of the generated log messages

`sbt "runMain TypeCount"`

#### Character Count Job

The number of characters in each log message for each log message type

`sbt "runMain CharacterCount"`

### Run on AWS 

Here is a short video showing how to deploy to AWS's EMR service

[https://thekleinbottle.com/log-generator-tut/index.html](https://thekleinbottle.com/log-generator-tut/index.html)


