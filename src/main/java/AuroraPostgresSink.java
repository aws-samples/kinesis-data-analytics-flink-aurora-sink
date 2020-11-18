/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */


import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import model.MyUser;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.types.Row;
import serialization.MyUserDeserialization;

import java.io.IOException;
import java.sql.Types;
import java.util.Map;
import java.util.Properties;


public class AuroraPostgresSink {


    /**
     * Passed Arguments
     */
    public static String REGION;
    public static String INPUT_STREAM_NAME;
    public static String DRIVER_NAME;
    public static String CONNECTION_STRING;
    public static String USERNAME;
    public static String PASSWORD;
    public static String NAMESPACE;
    public static String TABLE;
    public static String DB_URL;
    public static String QUERY;

    private static DataStream<MyUser> createSourceFromStaticConfig(StreamExecutionEnvironment env) {
        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, REGION);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");
        inputProperties.setProperty(ConsumerConfigConstants.SHARD_USE_ADAPTIVE_READS, "true");

        return env.addSource(new FlinkKinesisConsumer<MyUser>(INPUT_STREAM_NAME, new MyUserDeserialization(), inputProperties));
    }




    private static StreamExecutionEnvironment getstreamingEnv() throws IOException {

        /**
         *      Set a local environment variable to switch for local development WebUI.
         *      This will be available in KDA on the Amazon Management Console when you deploy to KDA.
         */
        boolean isLocal = Boolean.parseBoolean(System.getenv("isLocal") != null ? System.getenv("isLocal") : "false");
        final StreamExecutionEnvironment env;
        if (isLocal) {
            // configure Web UI
            Configuration flinkConfig = new Configuration();
            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);

            // Explicitly set checkpointing configuration locally to mirror KDA Configuration
            env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
            env.setParallelism(6);
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }

        setApplicationConstants(isLocal);

        return env;
    }

    /**
     * Set these properties either in a resource (config.json) or on KDA in the Application Properties
     * @param isLocal
     * @throws IOException
     */
    private static void setApplicationConstants(boolean isLocal) throws IOException {
        Map<String, Properties> propertiesMap;

        if(isLocal)
        {
            propertiesMap = KinesisAnalyticsRuntime.getApplicationProperties(AuroraPostgresSink.class.getResource("config.json").getPath());
        }
        else
        {
             propertiesMap = KinesisAnalyticsRuntime.getApplicationProperties();
        }


        Properties properties = propertiesMap.get("args");

        REGION = String.valueOf(properties.getOrDefault("region", "us-east-1"));
        INPUT_STREAM_NAME = String.valueOf(properties.getOrDefault("input_stream_name", "my-input-stream"));
        DRIVER_NAME = "org.postgresql.Driver";

        //database connection string jdbc:postgresql://host:port
        CONNECTION_STRING = String.valueOf(properties.get("connection_string"));

        USERNAME = String.valueOf(properties.get("username"));
        PASSWORD = String.valueOf(properties.get("password"));
        NAMESPACE =  String.valueOf(properties.get("namespace"));
        TABLE =  String.valueOf(properties.get("table"));
        DB_URL = new StringBuffer().append(CONNECTION_STRING)
                .append("/").append(NAMESPACE).append("?")
                .append("user=").append(USERNAME)
                .append("&password=").append(PASSWORD).toString();

        QUERY = "insert into " + TABLE + " values (?, ?, ?)";
    }


    static void writeToAurora() throws Exception
    {
        StreamExecutionEnvironment env = getstreamingEnv();
        DataStream<MyUser> source = createSourceFromStaticConfig(env);

        //load the driver
        Class.forName(DRIVER_NAME);

        JDBCOutputFormat jdbcOutput = JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername(DRIVER_NAME)
                .setDBUrl(DB_URL)
                .setQuery(QUERY)
                .setUsername(USERNAME)
                .setPassword(PASSWORD)
                .setSqlTypes(new int[] {Types.VARCHAR, Types.INTEGER, Types.VARCHAR})
                .setBatchInterval(100)  // batch inserts to postgres can be customized here.
                .finish();

        DataStream<Row> rows = source.map((user) -> {
            Row row = new Row(3);
            row.setField(0, user.getName());
            row.setField(1, user.getAge());
            row.setField(2, user.getLocation());
            return row;
        });

        rows.writeUsingOutputFormat(jdbcOutput);


        env.execute();


    }


    public static void main(String[] args) throws Exception {
        writeToAurora();

    }


}
