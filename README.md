# DataEngineerChallenge

This is an interview challenge for PayPay. Please feel free to fork. Pull Requests will be ignored.

The challenge is to make make analytical observations about the data using the distributed tools below.

## Processing & Analytical goals:

1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session.
    https://en.wikipedia.org/wiki/Session_(web_analytics)
2. Determine the average session time
3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
4. Find the most engaged users, ie the IPs with the longest session times

## Tools used:
- Spark (Java)

### Additional notes:
- IP addresses do not guarantee distinct users, but this is the limitation of the data. As a bonus, consider what additional data would help make better analytical conclusions
- For this dataset, the sessionization uses time window of 15 minutes.
- The log file was taken from an AWS Elastic Load Balancer:
http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html#access-log-entry-format

## How to Run

### Prerequisite

1. Java 1.8 installed
1. `JAVA_HOME` is set to Java 1.8 directory
1. Spark 2.x.x installed
1. Maven installed

### Unarchive Log Data

```shell script
gzip -d 2015_07_22_mktplace_shop_web_log_sample.log.gz
```

### Compile, Test, and Build

```shell script
mvn clean package
```

### Run

```shell script
java -jar target/DataEngineerChallenge-1.0-SNAPSHOT.jar
```
