package at.grahsl.kafka.connect.mongodb.processor.field.renaming;

public class RegExpSettings {

    public String regexp;
    public String pattern;
    public String replace;

    public RegExpSettings() {}

    public RegExpSettings(String regexp, String pattern, String replace) {
        this.regexp = regexp;
        this.pattern = pattern;
        this.replace = replace;
    }

    @Override
    public String toString() {
        return "RegExpSettings{" +
                "regexp='" + regexp + '\'' +
                ", pattern='" + pattern + '\'' +
                ", replace='" + replace + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RegExpSettings that = (RegExpSettings) o;

        if (regexp != null ? !regexp.equals(that.regexp) : that.regexp != null) return false;
        if (pattern != null ? !pattern.equals(that.pattern) : that.pattern != null) return false;
        return replace != null ? replace.equals(that.replace) : that.replace == null;
    }

    @Override
    public int hashCode() {
        int result = regexp != null ? regexp.hashCode() : 0;
        result = 31 * result + (pattern != null ? pattern.hashCode() : 0);
        result = 31 * result + (replace != null ? replace.hashCode() : 0);
        return result;
    }
}
