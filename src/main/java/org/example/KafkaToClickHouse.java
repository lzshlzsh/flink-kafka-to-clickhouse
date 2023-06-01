/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import org.example.utils.FileUtils;
import org.example.utils.JobUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/** KafkaToClickHouse demo. */
public class KafkaToClickHouse {
    private static final Logger logger = LoggerFactory.getLogger(KafkaToClickHouse.class);

    public static void main(String[] args) throws Exception {
        // 配置参数解析
        final ParameterTool parameterTool = JobUtils.parseParameter(KafkaToClickHouse.class, args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        JobUtils.configJobCommon(env, parameterTool);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        tEnv.createTemporarySystemFunction(
                SplitFunction3.class.getSimpleName(), SplitFunction3.class);

        tEnv.createTemporarySystemFunction(
                SplitFunction4.class.getSimpleName(), SplitFunction4.class);

        tEnv.executeSql(
                "CREATE TABLE `kafka_raw` (\n"
                        + "  data string\n"
                        + ")\n"
                        + "WITH (\n"
                        + "     'connector' = 'kafka',\n"
                        + "     'properties.bootstrap.servers' = '127.0.0.1:9092',\n"
                        + "     'topic' = 'test-topic',\n"
                        + "     'properties.group.id' = 'test_group',\n"
                        + "     'scan.startup.mode' = 'earliest-offset',\n"
                        + "     'scan.topic-partition-discovery.interval' = '10s',\n"
                        + "     'format' = 'raw'\n"
                        + ");");

        // 日志内容
        // tableB|1|2|3|4
        // tableA|2|3|4
        DataStream<Row> generalSource =
                tEnv.toDataStream(tEnv.sqlQuery("SELECT * FROM `kafka_raw`"))
                        .map(
                                (MapFunction<Row, Row>)
                                        value ->
                                                Row.of(
                                                        ((String) value.getField(0))
                                                                .split("\\|", 2)))
                        .returns(Types.ROW(Types.STRING, Types.STRING));
        tEnv.createTemporaryView("kafka_source", generalSource);

        // TODO: 日志数据里的表与 sink 表文件名，dml 文件名，udtf 的映射关系
        Map<String, Tuple3<String, String, String>> tableMap = new HashMap<>();
        tableMap.put(
                "table1",
                Tuple3.of(
                        "clickhouse_sink_1.sql",
                        "clickhouse_dml_1.sql",
                        SplitFunction3.class.getSimpleName()));
        tableMap.put(
                "table2",
                Tuple3.of(
                        "clickhouse_sink_2.sql",
                        "clickhouse_dml_2.sql",
                        SplitFunction4.class.getSimpleName()));

        StatementSet statementSet = tEnv.createStatementSet();
        for (Map.Entry<String, Tuple3<String, String, String>> table : tableMap.entrySet()) {
            final String ddl = FileUtils.readResourceFile("sql/" + table.getValue().f0);
            tEnv.executeSql(ddl.replaceAll("\\$\\{TABLE_NAME}", table.getKey()));

            final String dml = FileUtils.readResourceFile("sql/" + table.getValue().f1);
            statementSet.addInsertSql(
                    dml.replaceAll("\\$\\{TABLE_NAME}", table.getKey())
                            .replaceAll("\\$\\{UDTF_NAME}", table.getValue().f2));
        }

        statementSet.execute();
    }

    /** */
    @FunctionHint(output = @DataTypeHint("ROW<f1 STRING, f2 STRING, f3 STRING>"))
    public static class SplitFunction3 extends TableFunction<Row> {
        public void eval(String str) {
            collect(Row.of(str.split("\\|")));
        }
    }

    /** */
    @FunctionHint(output = @DataTypeHint("ROW<f1 STRING, f2 STRING, f3 STRING, f4 STRING>"))
    public static class SplitFunction4 extends TableFunction<Row> {
        public void eval(String str) {
            collect(Row.of(str.split("\\|")));
        }
    }
}
