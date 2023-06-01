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

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Files;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** */
public class FileUtils {
    public static String readResourceFile(String resourceName) throws Exception {
        URL url = FileUtils.class.getClassLoader().getResource(resourceName);
        checkNotNull(url);
        String protocol = url.getProtocol();
        String fileContent = "";
        if (protocol.equals("jar")) {
            BufferedReader in =
                    new BufferedReader(
                            new InputStreamReader(
                                    Objects.requireNonNull(
                                            FileUtils.class
                                                    .getClassLoader()
                                                    .getResourceAsStream(resourceName))));
            StringBuilder stringBuilder = new StringBuilder();
            in.lines().forEach(line -> stringBuilder.append(line).append("\n"));
            fileContent = stringBuilder.toString();
        } else if (protocol.equals("file")) {
            fileContent = String.join("\n", Files.readAllLines((new File(url.toURI())).toPath()));
        }
        return fileContent;
    }
}
