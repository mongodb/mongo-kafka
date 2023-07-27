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

package com.mongodb.kafka.connect.source.topic.mapping;

import static com.mongodb.kafka.connect.source.MongoSourceConfig.TOPIC_NAMESPACE_MAP_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.TOPIC_PREFIX_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.TOPIC_SEPARATOR_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.TOPIC_SUFFIX_CONFIG;
import static com.mongodb.kafka.connect.util.Assertions.assertFalse;
import static com.mongodb.kafka.connect.util.BsonDocumentFieldLookup.fieldLookup;
import static com.mongodb.kafka.connect.util.ConfigHelper.documentFromString;
import static java.lang.String.format;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.bson.BsonDocument;
import org.bson.Document;

import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.lang.Nullable;

import com.mongodb.kafka.connect.source.MongoSourceConfig;
import com.mongodb.kafka.connect.util.ConnectConfigException;

@NotThreadSafe
public class DefaultTopicMapper implements TopicMapper {
  private static final String DB_FIELD_PATH = "ns.db";
  private static final String COLL_FIELD_PATH = "ns.coll";
  private static final String REGEX_NAMESPACE_PATTERN_MARK = "/";
  private static final String WILDCARD_NAMESPACE_PATTERN = "*";
  private static final char NAMESPACE_SEPARATOR = '.';

  private String separator;
  private String prefix;
  private String suffix;
  private Map<String, String> simplePairs;
  private List<Entry<Matcher, TopicNameTemplate>> regexPairs;
  @Nullable private String undecoratedWildcardTopicName;
  private final Map<String, String> namespaceTopicCache;

  public DefaultTopicMapper() {
    namespaceTopicCache = new HashMap<>();
  }

  @Override
  public void configure(final MongoSourceConfig config) {
    ConfigExceptionSupplier configExceptionSupplier =
        message ->
            new ConnectConfigException(
                TOPIC_NAMESPACE_MAP_CONFIG, config.getString(TOPIC_NAMESPACE_MAP_CONFIG), message);
    String prefix = config.getString(TOPIC_PREFIX_CONFIG);
    String suffix = config.getString(TOPIC_SUFFIX_CONFIG);

    this.separator = config.getString(TOPIC_SEPARATOR_CONFIG);
    this.prefix = prefix.isEmpty() ? prefix : prefix + separator;
    this.suffix = suffix.isEmpty() ? suffix : separator + suffix;
    Document topicNamespaceMap =
        documentFromString(config.getString(TOPIC_NAMESPACE_MAP_CONFIG)).orElse(new Document());
    simplePairs = new HashMap<>();
    regexPairs = new ArrayList<>();
    for (Entry<String, Object> pair : topicNamespaceMap.entrySet()) {
      String namespacePattern = pair.getKey();
      Object value = pair.getValue();
      if (!(value instanceof String)) {
        throw configExceptionSupplier.apply("All values must be strings");
      }
      String topicNameTemplate = (String) pair.getValue();
      if (namespacePattern.equals(WILDCARD_NAMESPACE_PATTERN)) {
        undecoratedWildcardTopicName = topicNameTemplate;
      } else if (namespacePattern.startsWith(REGEX_NAMESPACE_PATTERN_MARK)) {
        regexPairs.add(
            new SimpleImmutableEntry<>(
                regexMatcher(
                    namespacePattern.substring(REGEX_NAMESPACE_PATTERN_MARK.length()),
                    configExceptionSupplier),
                new TopicNameTemplate(topicNameTemplate, separator, configExceptionSupplier)));
      } else {
        if (namespacePattern.contains(REGEX_NAMESPACE_PATTERN_MARK)) {
          throw configExceptionSupplier.apply(
              format(
                  "'%s' is not allowed in a simple namespace pattern",
                  REGEX_NAMESPACE_PATTERN_MARK));
        }
        simplePairs.put(namespacePattern, topicNameTemplate);
      }
    }
    // We have to remove pairs with empty topic name templates here to maintain the order of pairs
    // in the presence of duplicates
    // as documented by the `topic.namespace.map` configuration property.
    simplePairs.entrySet().removeIf(pair -> pair.getValue().isEmpty());
    regexPairs.removeIf(pair -> pair.getValue().isEmpty());
    if (undecoratedWildcardTopicName != null && undecoratedWildcardTopicName.isEmpty()) {
      undecoratedWildcardTopicName = null;
    }
  }

  /**
   * @param changeStreamDocument {@inheritDoc}
   *     <p>This implementation expects the {@code changeStreamDocument} to have a field named
   *     {@code ns} in the {@linkplain
   *     com.mongodb.client.model.changestream.ChangeStreamDocument#getNamespaceDocument() change
   *     stream format}. If there is no such field, or if the field does not specify a
   *     non-{@linkplain String#isEmpty() empty} MongoDB database name, the method returns an
   *     {@linkplain String#isEmpty() empty} string.
   */
  @Override
  public String getTopic(final BsonDocument changeStreamDocument) {

    String dbName = getStringFromPath(DB_FIELD_PATH, changeStreamDocument);
    if (dbName.isEmpty()) {
      return "";
    }
    String collName = getStringFromPath(COLL_FIELD_PATH, changeStreamDocument);
    String namespace = namespace(dbName, collName);

    String cachedTopic = namespaceTopicCache.get(namespace);
    if (cachedTopic == null) {
      cachedTopic = decorateTopicName(getUndecoratedTopicName(dbName, collName));
      namespaceTopicCache.put(namespace, cachedTopic);
    }
    return cachedTopic;
  }

  private String getStringFromPath(
      final String fieldPath, final BsonDocument changeStreamDocument) {
    return fieldLookup(fieldPath, changeStreamDocument)
        .map(bsonValue -> bsonValue.isString() ? bsonValue.asString().getValue() : "")
        .orElse("");
  }

  private String getUndecoratedTopicName(final String dbName, final String collName) {
    assertFalse(dbName.isEmpty());
    String namespace = namespace(dbName, collName);
    String topicNameTemplate = simplePairs.get(namespace);
    if (topicNameTemplate != null) {
      return topicNameTemplate;
    }

    topicNameTemplate = simplePairs.get(dbName);
    if (topicNameTemplate != null) {
      return undecoratedTopicName(topicNameTemplate, collName);
    }

    String undecoratedTopicName =
        regexPairs.stream()
            .filter(pair -> pair.getKey().reset(namespace).matches())
            .findFirst()
            .map(pair -> pair.getValue().compute(dbName, collName))
            .orElse(null);
    if (undecoratedTopicName != null) {
      return undecoratedTopicName;
    }

    if (undecoratedWildcardTopicName != null) {
      return undecoratedWildcardTopicName;
    }
    return undecoratedTopicName(dbName, collName);
  }

  private static String namespace(final String dbName, final String collName) {
    return collName.isEmpty() ? dbName : dbName + NAMESPACE_SEPARATOR + collName;
  }

  private String undecoratedTopicName(
      final String dbNameOrMappedTopicNamePart, final String collName) {
    return collName.isEmpty()
        ? dbNameOrMappedTopicNamePart
        : dbNameOrMappedTopicNamePart + separator + collName;
  }

  private String decorateTopicName(final String undecoratedTopicName) {
    return prefix + undecoratedTopicName + suffix;
  }

  private static Matcher regexMatcher(
      final String regex, final ConfigExceptionSupplier configExceptionSupplier)
      throws ConnectConfigException {
    try {
      return Pattern.compile(regex).matcher("");
    } catch (PatternSyntaxException e) {
      throw configExceptionSupplier.apply(e.getMessage());
    }
  }

  @FunctionalInterface
  private interface ConfigExceptionSupplier extends Function<String, ConnectConfigException> {}

  private static final class TopicNameTemplate {
    private static final char START_VAR_NAME = '{';
    private static final char END_VAR_NAME = '}';

    private final String compiledTemplate;
    private final ArrayList<Entry<Integer, VarName>> varExpansions;
    private final String separator;
    private final StringBuilder builder;

    TopicNameTemplate(
        final String template,
        final String separator,
        final ConfigExceptionSupplier configExceptionSupplier)
        throws ConnectConfigException {
      builder = new StringBuilder(template.length());
      varExpansions = new ArrayList<>();
      compiledTemplate =
          compile(template, separator, builder, varExpansions, configExceptionSupplier);
      this.separator = separator;
    }

    String compute(final String dbName, final String collName) {
      assertFalse(isEmpty());
      if (varExpansions.isEmpty()) {
        return compiledTemplate;
      }
      builder.setLength(0);
      builder.append(compiledTemplate);
      // As we expand variables, the index of the following expansions offsets correspondingly,
      // we use `varExpansionIdxOffset` to track this offset.
      int varExpansionIdxOffset = 0;
      for (Entry<Integer, VarName> varExpansion : varExpansions) {
        int idx = varExpansion.getKey();
        VarName varName = varExpansion.getValue();
        final String varValue = varName.computeValue(dbName, collName, separator);
        if (!varValue.isEmpty()) {
          builder.insert(idx + varExpansionIdxOffset, varValue);
          varExpansionIdxOffset += varValue.length();
        }
      }
      return builder.toString();
    }

    boolean isEmpty() {
      return compiledTemplate.isEmpty()
          // if `compiledTemplate` is empty but `varExpansions` is not,
          // then this template still may produce non-empty results
          && varExpansions.isEmpty();
    }

    @Override
    public String toString() {
      return "TopicNameTemplate{"
          + "compiledTemplate='"
          + compiledTemplate
          + '\''
          + ", varExpansions="
          + varExpansions
          + ", separator='"
          + separator
          + '\''
          + '}';
    }

    private static String compile(
        final String template,
        final String separator,
        final StringBuilder compiledTemplateBuilder,
        final ArrayList<Entry<Integer, VarName>> varExpansions,
        final ConfigExceptionSupplier configExceptionSupplier)
        throws ConnectConfigException {
      String varExpansionErrorMessageFormat =
          "Variable expansion syntax is violated, unexpected '%c'";
      int balance = 0;
      int firstUncompiledIdx = 0;
      int varPlaceholderStartIdx = -1;
      // The index of a variable expansion in `compiledTemplateBuilder`
      // differs from the index of the variable placeholder in `template`,
      // because a compiled template is different from the raw template:
      // - it does not have variable placeholders;
      // - it has the `VarName.SEP` variable expanded.
      // We use `varExpansionIdxOffset` to track the difference.
      int varExpansionIdxOffset = 0;
      char[] chars = template.toCharArray();
      for (int i = 0; i < chars.length; i++) {
        char c = chars[i];
        switch (c) {
          case START_VAR_NAME:
            balance += 1;
            throwIfInvalidBalance(
                balance, 0, 1, varExpansionErrorMessageFormat, configExceptionSupplier);
            varPlaceholderStartIdx = i;
            break;
          case END_VAR_NAME:
            balance -= 1;
            throwIfInvalidBalance(
                balance, 0, 1, varExpansionErrorMessageFormat, configExceptionSupplier);
            int varPlaceholderLength = i - varPlaceholderStartIdx + 1;
            VarName varName =
                VarName.of(
                    template.substring(varPlaceholderStartIdx + 1, i), configExceptionSupplier);
            compiledTemplateBuilder.append(template, firstUncompiledIdx, varPlaceholderStartIdx);
            if (varName == VarName.SEP) {
              compiledTemplateBuilder.append(separator);
              varExpansionIdxOffset -= separator.length();
            } else {
              varExpansions.add(
                  new SimpleImmutableEntry<>(
                      varPlaceholderStartIdx - varExpansionIdxOffset, varName));
            }
            varExpansionIdxOffset += varPlaceholderLength;
            firstUncompiledIdx = i + 1;
            break;
          default:
            // nothing to do
        }
      }
      throwIfInvalidBalance(balance, 0, 0, varExpansionErrorMessageFormat, configExceptionSupplier);
      compiledTemplateBuilder.append(template, firstUncompiledIdx, template.length());
      return compiledTemplateBuilder.toString();
    }

    private static void throwIfInvalidBalance(
        final int balance,
        final int minBalance,
        final int maxBalance,
        final String errorMessageFormat,
        final ConfigExceptionSupplier configExceptionSupplier)
        throws ConnectConfigException {
      if (balance < minBalance) {
        throw configExceptionSupplier.apply(format(Locale.ROOT, errorMessageFormat, END_VAR_NAME));
      } else if (balance > maxBalance) {
        throw configExceptionSupplier.apply(
            format(Locale.ROOT, errorMessageFormat, START_VAR_NAME));
      }
    }

    private enum VarName {
      DB((dbName, collName, separator) -> dbName),
      SEP((dbName, collName, separator) -> separator),
      COLL((dbName, collName, separator) -> collName),
      SEP_COLL((dbName, collName, separator) -> collName.isEmpty() ? "" : separator + collName),
      COLL_SEP((dbName, collName, separator) -> collName.isEmpty() ? "" : collName + separator),
      SEP_COLL_SEP(
          (dbName, collName, separator) ->
              collName.isEmpty() ? "" : separator + collName + separator);

      private static final Set<String> SUPPORTED_VAR_NAMES =
          Stream.of(VarName.values())
              .map(VarName::name)
              .map(varName -> varName.toLowerCase(Locale.ROOT))
              .collect(Collectors.toSet());

      private final ValueComputer valueComputer;

      VarName(final ValueComputer computer) {
        this.valueComputer = computer;
      }

      String computeValue(final String dbName, final String collName, final String separator) {
        return this.valueComputer.compute(dbName, collName, separator);
      }

      static VarName of(final String s, final ConfigExceptionSupplier configExceptionSupplier)
          throws ConnectConfigException {
        if (!SUPPORTED_VAR_NAMES.contains(s)) {
          // We do this to enforce case-sensitivity,
          // otherwise we could have called `VarName.valueOf`
          // and checked for `IllegalArgumentException`s.
          throw configExceptionSupplier.apply(s + " is not a supported variable name");
        }
        return VarName.valueOf(s.toUpperCase(Locale.ROOT));
      }

      @FunctionalInterface
      private interface ValueComputer {
        String compute(String dbName, String collName, String separator);
      }
    }
  }
}
