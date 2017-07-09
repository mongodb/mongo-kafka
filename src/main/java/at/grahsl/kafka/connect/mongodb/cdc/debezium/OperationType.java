/*
 * Copyright (c) 2017. Hans-Peter Grahsl (grahslhp@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package at.grahsl.kafka.connect.mongodb.cdc.debezium;

public enum OperationType {

    CREATE("c"),
    READ("r"),
    UPDATE("u"),
    DELETE("d");

    private final String text;

    OperationType(String text) {
        this.text = text;
    }

    String type() {
        return this.text;
    }

    public static OperationType fromText(String text) {
        switch(text) {
            case "c": return CREATE;
            case "r": return READ;
            case "u": return UPDATE;
            case "d": return DELETE;
            default:
                throw new IllegalArgumentException(
                        "error: unknown operation type " + text
                );
        }
    }

}