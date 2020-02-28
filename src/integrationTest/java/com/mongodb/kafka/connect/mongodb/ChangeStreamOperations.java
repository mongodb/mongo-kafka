/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mongodb.kafka.connect.mongodb;

import static java.util.stream.IntStream.rangeClosed;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.bson.Document;

public class ChangeStreamOperations {
    private static final ChangeStreamOperation DROP_DATABASE = new DropDatabase();
    private static final ChangeStreamOperation DROP = new Drop();
    private static final ChangeStreamOperation UNKNOWN = new Unknown();


    public interface ChangeStreamOperation {
    }

    public static ChangeStreamOperation createDropCollection() {
        return DROP;
    }

    public static ChangeStreamOperation createDropDatabase() {
        return DROP_DATABASE;
    }

    @SafeVarargs
    public static  List<ChangeStreamOperation> concat(List<? extends ChangeStreamOperation> list,
                                                      List<ChangeStreamOperation>... args){
        return Stream.concat(list.stream(), Stream.of(args).flatMap(List::stream))
                .collect(Collectors.toList());
    }

    public static List<ChangeStreamOperation> createInserts(final int start, final int end) {
        return rangeClosed(start, end).mapToObj(ChangeStreamOperations::createInsert).collect(Collectors.toList());
    }

    public static ChangeStreamOperation createInsert(final int id) {
        return new Insert(id);
    }

    public static ChangeStreamOperation createChangeStreamOperation(final String changeStreamJson) {
        Document document = Document.parse(changeStreamJson);
        ChangeStreamOperation changeStreamOperation;
        switch (document.get("operationType", "unknown").toLowerCase()) {
            case "dropdatabase":
                changeStreamOperation = DROP_DATABASE;
                break;
            case "drop":
                changeStreamOperation = DROP;
                break;
            case "insert":
                changeStreamOperation = new Insert(document.get("documentKey", new Document()).getInteger("_id", -1));
                break;
            default:
                changeStreamOperation = UNKNOWN;
        }
        return changeStreamOperation;
    }

    private static class Drop implements ChangeStreamOperation {
        public Drop() {
        }

        @Override
        public String toString() {
            return "DropCollection{}";
        }
    }

    private static class DropDatabase implements ChangeStreamOperation {
        public DropDatabase() {
        }

        @Override
        public String toString() {
            return "DropDatabase{}";
        }
    }

    public static class Insert implements ChangeStreamOperation {
        private final int id;

        public Insert(final int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Insert that = (Insert) o;
            return id == that.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }

        @Override
        public String toString() {
            return "Insert{" +
                    "id=" + id +
                    '}';
        }
    }

    private static class Unknown implements ChangeStreamOperation {
    }

    private ChangeStreamOperations() {
    }
}
