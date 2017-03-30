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
