package at.grahsl.kafka.connect.mongodb;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.regex.Pattern;

import static at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig.ValidatorWithOperators;
import static org.junit.jupiter.api.Assertions.assertThrows;

@RunWith(JUnitPlatform.class)
public class ValidatorWithOperatorsTest {


    public static final String NAME = "name";
    public static final Object ANY_VALUE = null;

    final ValidatorWithOperators PASS = (name, value) -> {
        // ignore, always passes
    };

    final ValidatorWithOperators FAIL = (name, value) -> {
        throw new ConfigException(name, value, "always fails");
    };

    @Test
    @DisplayName("validate empty string")
    public void emptyString() {
        ValidatorWithOperators validator = MongoDbSinkConnectorConfig.emptyString();
        validator.ensureValid(NAME, "");
    }

    @Test
    @DisplayName("invalidate non-empty string")
    public void invalidateNonEmptyString() {
        ValidatorWithOperators validator = MongoDbSinkConnectorConfig.emptyString();
        assertThrows(ConfigException.class, () -> validator.ensureValid(NAME, "value"));
    }

    @Test
    @DisplayName("validate regex")
    public void simpleRegex() {
        ValidatorWithOperators validator = MongoDbSinkConnectorConfig.matching(Pattern.compile("fo+ba[rz]"));
        validator.ensureValid(NAME, "foobar");
        validator.ensureValid(NAME, "foobaz");
    }

    @Test
    @DisplayName("invalidate regex")
    public void invalidateSimpleRegex() {
        ValidatorWithOperators validator = MongoDbSinkConnectorConfig.matching(Pattern.compile("fo+ba[rz]"));
        assertThrows(ConfigException.class, () -> validator.ensureValid(NAME, "foobax"));
        assertThrows(ConfigException.class, () -> validator.ensureValid(NAME, "fbar"));
    }

    @Test
    @DisplayName("validate arithmetic or")
    public void arithmeticOr() {
        PASS.or(PASS).ensureValid(NAME, ANY_VALUE);
        PASS.or(FAIL).ensureValid(NAME, ANY_VALUE);
        FAIL.or(PASS).ensureValid(NAME, ANY_VALUE);
        PASS.or(PASS).or(PASS).ensureValid(NAME, ANY_VALUE);
        PASS.or(PASS).or(FAIL).ensureValid(NAME, ANY_VALUE);
        PASS.or(FAIL).or(PASS).ensureValid(NAME, ANY_VALUE);
        PASS.or(FAIL).or(FAIL).ensureValid(NAME, ANY_VALUE);
        FAIL.or(PASS).or(PASS).ensureValid(NAME, ANY_VALUE);
        FAIL.or(PASS).or(FAIL).ensureValid(NAME, ANY_VALUE);
        FAIL.or(FAIL).or(PASS).ensureValid(NAME, ANY_VALUE);
    }

    @Test
    @DisplayName("invalidate arithmetic or")
    public void invalidateArithmeticOr() {
        assertThrows(ConfigException.class, () -> FAIL.or(FAIL).ensureValid(NAME, ANY_VALUE));
        assertThrows(ConfigException.class, () -> FAIL.or(FAIL).or(FAIL).ensureValid(NAME, ANY_VALUE));
    }

    @Test
    @DisplayName("arithmetic and")
    public void arithmeticAnd() {
        PASS.and(PASS).ensureValid(NAME, ANY_VALUE);
        PASS.and(PASS).and(PASS).ensureValid(NAME, ANY_VALUE);
    }

    @Test
    @DisplayName("invalidate arithmetic and")
    public void invalidateArithmeticAnd() {
        assertThrows(ConfigException.class, () -> PASS.and(FAIL).ensureValid(NAME, ANY_VALUE));
        assertThrows(ConfigException.class, () -> FAIL.and(PASS).ensureValid(NAME, ANY_VALUE));
        assertThrows(ConfigException.class, () -> FAIL.and(FAIL).ensureValid(NAME, ANY_VALUE));
    }

}
