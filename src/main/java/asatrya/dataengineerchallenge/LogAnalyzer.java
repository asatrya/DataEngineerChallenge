package asatrya.dataengineerchallenge;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;

/**
 * Class responsible to do Logs Analyzing tasks
 */
public class LogAnalyzer {

    // start a spark context
    public static SparkSession spark = SparkSession.builder()
            .appName("Log Analyzer")
            .master("local[*]")
            .getOrCreate();

    private static int TIME_INTERVAL_THRESHOLD_IN_SECONDS = 15 * 60; // time interval threshold
    private static String LOG_FILE = "data/2015_07_22_mktplace_shop_web_log_sample.log"; // logfile path
    private static String TIME_ZONE = "UTC"; // time zone

    /**
     * Constructor
     */
    public LogAnalyzer() {
        spark.conf().set("spark.sql.session.timeZone", LogAnalyzer.TIME_ZONE);
    }

    /**
     * Parse raw log text to Dataset<AccessLogLine> Object
     * @param logFile
     * @return
     */
    public Dataset<AccessLogLine> parseLogLines(String logFile) {
        Dataset<String> logLines = spark.read().textFile(logFile).as(Encoders.STRING());
        Dataset<AccessLogLine> parsedLogLines = logLines.map(
                (MapFunction<String, AccessLogLine>) line -> AccessLogLine.parseFromLogLine(line),
                Encoders.bean(AccessLogLine.class));
        parsedLogLines = parsedLogLines.withColumn("timestamp",
                parsedLogLines.col("timestamp").cast(DataTypes.TimestampType)).as(Encoders.bean(AccessLogLine.class));
        return parsedLogLines;
    }

    /**
     * Prepare logs by enriching with `prev_timestamp`, `time_interval`, `is_new_session`, and `client_session_seq` columns
     * @param parsedLogLines
     * @param timeIntervalThresholdInSecond
     * @return sessionizedLogLines
     */
    public Dataset<AccessLogLine> prepareLogLines(Dataset<AccessLogLine> parsedLogLines, int timeIntervalThresholdInSecond) {
        // add `prev_timestamp` column
        Dataset<AccessLogLine> preparedLogLines = parsedLogLines.withColumn(
                "prev_timestamp",
                functions.lag(parsedLogLines.col("timestamp"), 1)
                        .over(Window.partitionBy("client_ip")
                                .orderBy("timestamp")))
                .as(Encoders.bean(AccessLogLine.class));

        // add `time_interval` column
        preparedLogLines = preparedLogLines.withColumn("time_interval",
                functions.unix_timestamp(preparedLogLines.col("timestamp"))
                        .minus(functions.unix_timestamp(preparedLogLines.col("prev_timestamp"))))
                .as(Encoders.bean(AccessLogLine.class));

        // add `is_new_session` column
        preparedLogLines = preparedLogLines.withColumn("is_new_session",
                functions.when(preparedLogLines.col("time_interval").$greater(timeIntervalThresholdInSecond)
                        .or(preparedLogLines.col("time_interval").isNull()), 1).otherwise(0))
                .as(Encoders.bean(AccessLogLine.class));

        // add `client_session_seq` column
        preparedLogLines = preparedLogLines.withColumn("client_session_seq",
                functions.sum(preparedLogLines.col("is_new_session"))
                        .over(Window.partitionBy("client_ip").orderBy("timestamp")))
                .as(Encoders.bean(AccessLogLine.class));

        return preparedLogLines;
    }

    /**
     * Sessionize the web log by IP. Sessionize = aggregate all page hits by visitor/IP during a session.
     * @param parsedLogLines
     * @return
     */
    public Dataset<Row> sessionizeLogLines(Dataset<AccessLogLine> parsedLogLines){
        Dataset<Row> sessionizedLogLines = parsedLogLines.groupBy("client_ip", "client_session_seq")
                .sum("time_interval")
                .withColumnRenamed("sum(time_interval)", "sum_time_interval");
        return sessionizedLogLines;
    }

    /**
     * Determine the average session time
     * Assumption: session that only contain single request is not counted
     * @param sessionizedLogLines
     * @return
     */
    public Dataset<Row> getAverageSessionTime(Dataset<Row> sessionizedLogLines){
        Dataset<Row> avgSessionTimeLogLines = sessionizedLogLines.select(functions.avg("sum_time_interval"));
        return avgSessionTimeLogLines;
    }

    /**
     * Determine the number unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
     * @param preparedLogLines
     * @return
     */
    public Dataset<Row> getUniqueUrlVisitsCount(Dataset<AccessLogLine> preparedLogLines){
        Dataset<Row> uniqueUrlVisitsCount = preparedLogLines.groupBy("client_ip", "client_session_seq")
                .agg(functions.countDistinct("request_url").alias("unique_url_count"));
        return uniqueUrlVisitsCount;
    }

    /**
     * List the most engaged users (IPs with the longest session times)
     * @param preparedLogLines
     * @return
     */
    public Dataset<Row> getMostEngangedUsers(Dataset<AccessLogLine> preparedLogLines){
        Dataset<Row> mostEngagedUsers = preparedLogLines.groupBy("client_ip")
                .sum("time_interval")
                .orderBy(functions.col("sum(time_interval)").desc())
                .withColumnRenamed("sum(time_interval)", "sum_time_interval");
        return mostEngagedUsers;
    }

    public static void main(String[] args) {
        LogAnalyzer logAnalyzer = new LogAnalyzer();

        // parse logs
        Dataset<AccessLogLine> parsedLogLines = logAnalyzer.parseLogLines(LogAnalyzer.LOG_FILE);

        // prepare logs
        Dataset<AccessLogLine> preparedLogLines = logAnalyzer.prepareLogLines(parsedLogLines, LogAnalyzer.TIME_INTERVAL_THRESHOLD_IN_SECONDS);
        System.out.println("Prepared Logs:");
        preparedLogLines.select("client_ip", "timestamp",
                "prev_timestamp", "time_interval",
                "is_new_session", "client_session_seq").show();

        // sessionize logs
        Dataset<Row> sessionizedLogLines = logAnalyzer.sessionizeLogLines(preparedLogLines);
        System.out.println("Sessionized Logs:");
        sessionizedLogLines.show();

        // calculate average session time
        Dataset<Row> avgSessionTime = logAnalyzer.getAverageSessionTime(sessionizedLogLines);
        System.out.println("Average Session Time:");
        avgSessionTime.show();

        // calculate unique URL visits count
        Dataset<Row> uniqueUrlVisits = logAnalyzer.getUniqueUrlVisitsCount(preparedLogLines);
        System.out.println("Unique URL Visit per Session:");
        uniqueUrlVisits.show();

        // list the most engaged users
        Dataset<Row> mostEngagedUsers = logAnalyzer.getMostEngangedUsers((preparedLogLines));
        System.out.println("Most Engaged Users:");
        mostEngagedUsers.show();

        spark.stop();
    }

}
