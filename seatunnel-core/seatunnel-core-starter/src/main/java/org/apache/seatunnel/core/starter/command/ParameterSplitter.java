/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.core.starter.command;

import com.beust.jcommander.converters.IParameterSplitter;

import java.util.ArrayList;
import java.util.List;

public class ParameterSplitter implements IParameterSplitter {

    @Override
    public List<String> split(String value) {

        List<String> result = new ArrayList<>();
        StringBuilder currentToken = new StringBuilder();
        boolean insideBrackets = false;
        boolean insideQuotes = false;

        for (char c : value.toCharArray()) {

            if (c == '[') {
                insideBrackets = true;
            } else if (c == ']') {
                insideBrackets = false;
            } else if (c == '"') {
                insideQuotes = !insideQuotes;
            }

            if (c == ',' && !insideQuotes && !insideBrackets) {
                result.add(currentToken.toString().trim());
                currentToken = new StringBuilder();
            } else {
                currentToken.append(c);
            }
        }

        if (currentToken.length() > 0) {
            result.add(currentToken.toString().trim());
        }

        return result;
    }
}
