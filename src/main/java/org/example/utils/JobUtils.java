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

package org.example.utils;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** */
public class JobUtils {
    private static final Logger LOG = LoggerFactory.getLogger(JobUtils.class);

    public static ParameterTool parseParameter(Class<?> clszz, String[] args) throws IOException {
        return ParameterTool.fromPropertiesFile(
                        clszz.getResourceAsStream("/" + clszz.getSimpleName() + ".properties"))
                .mergeWith(ParameterTool.fromArgs(args));
    }

    public static void configJobCommon(
            StreamExecutionEnvironment env, ParameterTool parameterTool) {
        // checkpoint配置
        if (parameterTool.getBoolean("flink.stream.checkpoint.enable", true)) {
            env.enableCheckpointing(
                    parameterTool.getLong("flink.stream.checkpoint.interval", 30_000));
            env.getCheckpointConfig()
                    .enableExternalizedCheckpoints(
                            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        }

        if (!parameterTool.getBoolean("pipeline.operator-chaining", true)) {
            env.disableOperatorChaining();
        }

        // 重启策略
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10_000));
    }
}
