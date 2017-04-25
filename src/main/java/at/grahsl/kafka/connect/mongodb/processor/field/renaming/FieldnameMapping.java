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

package at.grahsl.kafka.connect.mongodb.processor.field.renaming;

public class FieldnameMapping {

    public String oldName;
    public String newName;

    public FieldnameMapping() {}

    public FieldnameMapping(String oldName, String newName) {
        this.oldName = oldName;
        this.newName = newName;
    }

    @Override
    public String toString() {
        return "FieldnameMapping{" +
                "oldName='" + oldName + '\'' +
                ", newName='" + newName + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FieldnameMapping that = (FieldnameMapping) o;

        if (oldName != null ? !oldName.equals(that.oldName) : that.oldName != null) return false;
        return newName != null ? newName.equals(that.newName) : that.newName == null;
    }

    @Override
    public int hashCode() {
        int result = oldName != null ? oldName.hashCode() : 0;
        result = 31 * result + (newName != null ? newName.hashCode() : 0);
        return result;
    }
}
