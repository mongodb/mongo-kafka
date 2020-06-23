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
 *
 * Original Work: Apache License, Version 2.0, Copyright 2017 Hans-Peter Grahsl.
 */

package com.mongodb.kafka.connect.avro;

import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.SchemaStore;
import org.apache.avro.specific.SpecificData;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class TweetMsg extends org.apache.avro.specific.SpecificRecordBase
    implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -614364705592652792L;
  public static final org.apache.avro.Schema SCHEMA$ =
      new org.apache.avro.Schema.Parser()
          .parse(
              "{\"type\":\"record\",\"name\":\"TweetMsg\",\"namespace\":"
                  + "\"com.mongodb.kafka.connect.data.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},"
                  + "{\"name\":\"text\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},"
                  + "{\"name\":\"hashtags\",\"type\":{\"type\":\"array\",\"items\":"
                  + "{\"type\":\"string\",\"avro.java.string\":\"String\"}}}]}");

  public static org.apache.avro.Schema getClassSchema() {
    return SCHEMA$;
  }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<TweetMsg> ENCODER =
      new BinaryMessageEncoder<TweetMsg>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<TweetMsg> DECODER =
      new BinaryMessageDecoder<TweetMsg>(MODEL$, SCHEMA$);

  /** Return the BinaryMessageDecoder instance used by this class. */
  public static BinaryMessageDecoder<TweetMsg> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link
   * SchemaStore}.
   *
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   */
  public static BinaryMessageDecoder<TweetMsg> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<TweetMsg>(MODEL$, SCHEMA$, resolver);
  }

  /** Serializes this TweetMsg to a ByteBuffer. */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /** Deserializes a TweetMsg from a ByteBuffer. */
  public static TweetMsg fromByteBuffer(java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  private long id;
  private java.lang.String text;
  private java.util.List<java.lang.String> hashtags;

  /**
   * Default constructor. Note that this does not initialize fields to their default values from the
   * schema. If that is desired then one should use <code>newBuilder()</code>.
   */
  public TweetMsg() {}

  /**
   * All-args constructor.
   *
   * @param id The new value for id
   * @param text The new value for text
   * @param hashtags The new value for hashtags
   */
  public TweetMsg(
      java.lang.Long id, java.lang.String text, java.util.List<java.lang.String> hashtags) {
    this.id = id;
    this.text = text;
    this.hashtags = hashtags;
  }

  public org.apache.avro.Schema getSchema() {
    return SCHEMA$;
  }

  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
      case 0:
        return id;
      case 1:
        return text;
      case 2:
        return hashtags;
      default:
        throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value = "unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
      case 0:
        id = (java.lang.Long) value$;
        break;
      case 1:
        text = (java.lang.String) value$;
        break;
      case 2:
        hashtags = (java.util.List<java.lang.String>) value$;
        break;
      default:
        throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'id' field.
   *
   * @return The value of the 'id' field.
   */
  public java.lang.Long getId$1() {
    return id;
  }

  /**
   * Sets the value of the 'id' field.
   *
   * @param value the value to set.
   */
  public void setId$1(java.lang.Long value) {
    this.id = value;
  }

  /**
   * Gets the value of the 'text' field.
   *
   * @return The value of the 'text' field.
   */
  public java.lang.String getText() {
    return text;
  }

  /**
   * Sets the value of the 'text' field.
   *
   * @param value the value to set.
   */
  public void setText(java.lang.String value) {
    this.text = value;
  }

  /**
   * Gets the value of the 'hashtags' field.
   *
   * @return The value of the 'hashtags' field.
   */
  public java.util.List<java.lang.String> getHashtags() {
    return hashtags;
  }

  /**
   * Sets the value of the 'hashtags' field.
   *
   * @param value the value to set.
   */
  public void setHashtags(java.util.List<java.lang.String> value) {
    this.hashtags = value;
  }

  /**
   * Creates a new TweetMsg RecordBuilder.
   *
   * @return A new TweetMsg RecordBuilder
   */
  public static TweetMsg.Builder newBuilder() {
    return new TweetMsg.Builder();
  }

  /**
   * Creates a new TweetMsg RecordBuilder by copying an existing Builder.
   *
   * @param other The existing builder to copy.
   * @return A new TweetMsg RecordBuilder
   */
  public static TweetMsg.Builder newBuilder(TweetMsg.Builder other) {
    return new TweetMsg.Builder(other);
  }

  /**
   * Creates a new TweetMsg RecordBuilder by copying an existing TweetMsg instance.
   *
   * @param other The existing instance to copy.
   * @return A new TweetMsg RecordBuilder
   */
  public static TweetMsg.Builder newBuilder(TweetMsg other) {
    return new TweetMsg.Builder(other);
  }

  /** RecordBuilder for TweetMsg instances. */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<TweetMsg>
      implements org.apache.avro.data.RecordBuilder<TweetMsg> {

    private long id;
    private java.lang.String text;
    private java.util.List<java.lang.String> hashtags;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     *
     * @param other The existing Builder to copy.
     */
    private Builder(TweetMsg.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.id)) {
        this.id = data().deepCopy(fields()[0].schema(), other.id);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.text)) {
        this.text = data().deepCopy(fields()[1].schema(), other.text);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.hashtags)) {
        this.hashtags = data().deepCopy(fields()[2].schema(), other.hashtags);
        fieldSetFlags()[2] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing TweetMsg instance
     *
     * @param other The existing instance to copy.
     */
    private Builder(TweetMsg other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.id)) {
        this.id = data().deepCopy(fields()[0].schema(), other.id);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.text)) {
        this.text = data().deepCopy(fields()[1].schema(), other.text);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.hashtags)) {
        this.hashtags = data().deepCopy(fields()[2].schema(), other.hashtags);
        fieldSetFlags()[2] = true;
      }
    }

    /**
     * Gets the value of the 'id' field.
     *
     * @return The value.
     */
    public java.lang.Long getId$1() {
      return id;
    }

    /**
     * Sets the value of the 'id' field.
     *
     * @param value The value of 'id'.
     * @return This builder.
     */
    public TweetMsg.Builder setId$1(long value) {
      validate(fields()[0], value);
      this.id = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
     * Checks whether the 'id' field has been set.
     *
     * @return True if the 'id' field has been set, false otherwise.
     */
    public boolean hasId$1() {
      return fieldSetFlags()[0];
    }

    /**
     * Clears the value of the 'id' field.
     *
     * @return This builder.
     */
    public TweetMsg.Builder clearId$1() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
     * Gets the value of the 'text' field.
     *
     * @return The value.
     */
    public java.lang.String getText() {
      return text;
    }

    /**
     * Sets the value of the 'text' field.
     *
     * @param value The value of 'text'.
     * @return This builder.
     */
    public TweetMsg.Builder setText(java.lang.String value) {
      validate(fields()[1], value);
      this.text = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
     * Checks whether the 'text' field has been set.
     *
     * @return True if the 'text' field has been set, false otherwise.
     */
    public boolean hasText() {
      return fieldSetFlags()[1];
    }

    /**
     * Clears the value of the 'text' field.
     *
     * @return This builder.
     */
    public TweetMsg.Builder clearText() {
      text = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
     * Gets the value of the 'hashtags' field.
     *
     * @return The value.
     */
    public java.util.List<java.lang.String> getHashtags() {
      return hashtags;
    }

    /**
     * Sets the value of the 'hashtags' field.
     *
     * @param value The value of 'hashtags'.
     * @return This builder.
     */
    public TweetMsg.Builder setHashtags(java.util.List<java.lang.String> value) {
      validate(fields()[2], value);
      this.hashtags = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
     * Checks whether the 'hashtags' field has been set.
     *
     * @return True if the 'hashtags' field has been set, false otherwise.
     */
    public boolean hasHashtags() {
      return fieldSetFlags()[2];
    }

    /**
     * Clears the value of the 'hashtags' field.
     *
     * @return This builder.
     */
    public TweetMsg.Builder clearHashtags() {
      hashtags = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public TweetMsg build() {
      try {
        TweetMsg record = new TweetMsg();
        record.id = fieldSetFlags()[0] ? this.id : (java.lang.Long) defaultValue(fields()[0]);
        record.text = fieldSetFlags()[1] ? this.text : (java.lang.String) defaultValue(fields()[1]);
        record.hashtags =
            fieldSetFlags()[2]
                ? this.hashtags
                : (java.util.List<java.lang.String>) defaultValue(fields()[2]);
        return record;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<TweetMsg> WRITER$ =
      (org.apache.avro.io.DatumWriter<TweetMsg>) MODEL$.createDatumWriter(SCHEMA$);

  @Override
  public void writeExternal(java.io.ObjectOutput out) throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<TweetMsg> READER$ =
      (org.apache.avro.io.DatumReader<TweetMsg>) MODEL$.createDatumReader(SCHEMA$);

  @Override
  public void readExternal(java.io.ObjectInput in) throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }
}
