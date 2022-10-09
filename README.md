
# Log File Generator

This is a application that utilizes the Hadoop framework and AWS EMR service to distribute tasks
across multiple nodes for a 

### Requirements

This application was built running Hadoop 3.3.0, and Java 15.0.1

Hadoop was installed using this tutorial:
[https://brain-mentors.com/hadoopinstallation/](https://brain-mentors.com/hadoopinstallation/)

## Running the application

Download and install the above-mentioned requirements or deploy directly to AWS

### Type Distribution Job

The Type Distribution Job 

`sbt "runMain TypeDistribution"`

### Error Interval Sort Job

The Error Interval Sort Job will 

`sbt "runMain ErrorIntervalSort"`

### Type Count Job

The Type Count Job

`sbt "runMain TypeCount"`

### Character Count Job

Character Count Job 

`sbt "runMain CharacterCount"`

### Run on AWS 

`sbt clean compile assembly`


