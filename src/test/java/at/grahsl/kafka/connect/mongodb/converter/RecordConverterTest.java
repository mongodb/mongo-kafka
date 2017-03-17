package at.grahsl.kafka.connect.mongodb.converter;

import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.bson.*;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@RunWith(JUnitPlatform.class)
public class RecordConverterTest {

    public static String JSON_STRING_1;
    public static Schema OBJ_SCHEMA_1;
    public static Struct OBJ_STRUCT_1;
    public static Map OBJ_MAP_1;
    public static BsonDocument EXPECTED_BSON_DOC_BYTES_1;
    public static BsonDocument EXPECTED_BSON_DOC_RAW_1;

    @BeforeAll
    public static void initializeTestData() {

        JSON_STRING_1 =
                "{\"_id\":\"1234567890\"," +
                "\"myString\":\"some foo bla text\"," +
                "\"myInt\":42," +
                "\"myBoolean\":true," +
                "\"mySubDoc1\":{\"myString\":\"hello json\"}," +
                "\"myArray1\":[\"str_1\",\"str_2\",\"...\",\"str_N\"]," +
                "\"myArray2\":[{\"k\":\"a\",\"v\":1},{\"k\":\"b\",\"v\":2},{\"k\":\"c\",\"v\":3}]," +
                "\"mySubDoc2\":{\"k1\":9,\"k2\":8,\"k3\":7}," +
                "\"myBytes\":\"S2Fma2Egcm9ja3Mh\"," +
                "\"myDate\": 1489705200000," +
                "\"myTimestamp\": 1489705200000," +
                "\"myTime\": 946724400000, " +
                "\"myDecimal\": 12345.6789 }";

        OBJ_SCHEMA_1 = SchemaBuilder.struct()
                .field("_id", Schema.STRING_SCHEMA)
                .field("myString", Schema.STRING_SCHEMA)
                .field("myInt",Schema.INT32_SCHEMA)
                .field("myBoolean", Schema.BOOLEAN_SCHEMA)
                .field("mySubDoc1", SchemaBuilder.struct()
                                    .field("myString",Schema.STRING_SCHEMA)
                                    .build()
                )
                .field("myArray1", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                .field("myArray2",SchemaBuilder.array(SchemaBuilder.struct()
                                    .field("k",Schema.STRING_SCHEMA)
                                    .field("v",Schema.INT32_SCHEMA)
                                    .build())
                )
                .field("mySubDoc2", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
                .field("myBytes", Schema.BYTES_SCHEMA)
                .field("myDate", Date.SCHEMA)
                .field("myTimestamp", Timestamp.SCHEMA)
                .field("myTime", Time.SCHEMA)
                .field("myDecimal", Decimal.schema(0))
                .build();

        OBJ_STRUCT_1 = new Struct(OBJ_SCHEMA_1)
                .put("_id","1234567890")
                .put("myString","some foo bla text")
                .put("myInt",42)
                .put("myBoolean",true)
                .put("mySubDoc1",new Struct(OBJ_SCHEMA_1.field("mySubDoc1").schema())
                                    .put("myString","hello json")
                )
                .put("myArray1",Arrays.asList("str_1","str_2","...","str_N"))
                .put("myArray2", Arrays.asList(
                        new Struct(OBJ_SCHEMA_1.field("myArray2").schema().valueSchema())
                                .put("k","a").put("v",1),
                        new Struct(OBJ_SCHEMA_1.field("myArray2").schema().valueSchema())
                                .put("k","b").put("v",2),
                        new Struct(OBJ_SCHEMA_1.field("myArray2").schema().valueSchema())
                                .put("k","c").put("v",3)
                        )
                )
                .put("mySubDoc2",new HashMap<String,Integer>(){{ put("k1",9); put("k2",8); put("k3",7);}})
                .put("myBytes", new byte[]{75, 97, 102, 107, 97, 32, 114, 111, 99, 107, 115, 33})
                .put("myDate", java.util.Date.from(ZonedDateTime.of(
                                LocalDate.of(2017,3,17), LocalTime.MIDNIGHT, ZoneOffset.systemDefault()
                            ).toInstant())
                )
                .put("myTimestamp", java.util.Date.from(ZonedDateTime.of(
                        LocalDate.of(2017,3,17), LocalTime.MIDNIGHT, ZoneOffset.systemDefault()
                        ).toInstant())
                )
                .put("myTime", java.util.Date.from(ZonedDateTime.of(
                        LocalDate.of(2000,1,1), LocalTime.NOON, ZoneOffset.systemDefault()
                        ).toInstant())
                )
                .put("myDecimal", new BigDecimal("12345.6789"));

        OBJ_MAP_1 = new LinkedHashMap<>();
        OBJ_MAP_1.put("_id","1234567890");
        OBJ_MAP_1.put("myString","some foo bla text");
        OBJ_MAP_1.put("myInt",42);
        OBJ_MAP_1.put("myBoolean",true);
        OBJ_MAP_1.put("mySubDoc1",new HashMap<Object,Object>(){{put("myString","hello json");}});
        OBJ_MAP_1.put("myArray1",Arrays.asList("str_1","str_2","...","str_N"));
        OBJ_MAP_1.put("myArray2", Arrays.asList(
                new HashMap<Object,Object>(){{put("k","a");put("v",1);}},
                new HashMap<Object,Object>(){{put("k","b");put("v",2);}},
                new HashMap<Object,Object>(){{put("k","c");put("v",3);}}
                )
        );
        OBJ_MAP_1.put("mySubDoc2",new HashMap<String,Integer>(){{ put("k1",9); put("k2",8); put("k3",7);}});
        OBJ_MAP_1.put("myBytes", new byte[]{75, 97, 102, 107, 97, 32, 114, 111, 99, 107, 115, 33});
        OBJ_MAP_1.put("myDate", java.util.Date.from(ZonedDateTime.of(
                LocalDate.of(2017,3,17), LocalTime.MIDNIGHT, ZoneOffset.systemDefault()
                ).toInstant())
        );
        OBJ_MAP_1.put("myTimestamp", java.util.Date.from(ZonedDateTime.of(
                LocalDate.of(2017,3,17), LocalTime.MIDNIGHT, ZoneOffset.systemDefault()
                ).toInstant())
        );
        OBJ_MAP_1.put("myTime", java.util.Date.from(ZonedDateTime.of(
                LocalDate.of(2000,1,1), LocalTime.NOON, ZoneOffset.systemDefault()
                ).toInstant())
        );
        //NOTE: as of now the BSON codec package seems to be missing a BigDecimalCodec
        // thus I'm cheating a little by using a Decimal128 here...
        OBJ_MAP_1.put("myDecimal", Decimal128.parse("12345.6789"));

        EXPECTED_BSON_DOC_BYTES_1 = new BsonDocument()
                .append("_id", new BsonString("1234567890"))
                .append("myString", new BsonString("some foo bla text"))
                .append("myInt", new BsonInt32(42))
                .append("myBoolean", new BsonBoolean(true))
                .append("mySubDoc1", new BsonDocument("myString", new BsonString("hello json")))
                .append("myArray1", new BsonArray(Arrays.asList(
                        new BsonString("str_1"),
                        new BsonString("str_2"),
                        new BsonString("..."),
                        new BsonString("str_N")))
                )
                .append("myArray2", new BsonArray(Arrays.asList(
                        new BsonDocument("k", new BsonString("a")).append("v", new BsonInt32(1)),
                        new BsonDocument("k", new BsonString("b")).append("v", new BsonInt32(2)),
                        new BsonDocument("k", new BsonString("c")).append("v", new BsonInt32(3))))
                ).append("mySubDoc2", new BsonDocument("k1", new BsonInt32(9))
                        .append("k2", new BsonInt32(8))
                        .append("k3", new BsonInt32(7))
                )
                .append("myBytes", new BsonBinary(new byte[]{75, 97, 102, 107, 97, 32, 114, 111, 99, 107, 115, 33}))
                .append("myDate", new BsonDateTime(
                        java.util.Date.from(ZonedDateTime.of(
                                LocalDate.of(2017,3,17), LocalTime.MIDNIGHT, ZoneOffset.systemDefault()
                        ).toInstant()).getTime()
                ))
                .append("myTimestamp", new BsonDateTime(
                        java.util.Date.from(ZonedDateTime.of(
                                LocalDate.of(2017,3,17), LocalTime.MIDNIGHT, ZoneOffset.systemDefault()
                        ).toInstant()).getTime()
                ))
                .append("myTime", new BsonDateTime(
                        java.util.Date.from(ZonedDateTime.of(
                                LocalDate.of(2000,1,1), LocalTime.NOON, ZoneOffset.systemDefault()
                        ).toInstant()).getTime()
                ))
                .append("myDecimal", new BsonDecimal128(new Decimal128(new BigDecimal("12345.6789"))));

        EXPECTED_BSON_DOC_RAW_1 = EXPECTED_BSON_DOC_BYTES_1.clone();
        EXPECTED_BSON_DOC_RAW_1.replace("myBytes",new BsonString("S2Fma2Egcm9ja3Mh"));
        EXPECTED_BSON_DOC_RAW_1.replace("myDate",new BsonInt64(1489705200000L));
        EXPECTED_BSON_DOC_RAW_1.replace("myTimestamp",new BsonInt64(1489705200000L));
        EXPECTED_BSON_DOC_RAW_1.replace("myTime",new BsonInt64(946724400000L));
        EXPECTED_BSON_DOC_RAW_1.replace("myDecimal", new BsonDouble(12345.6789));

    }

    @Test
    @DisplayName("test raw json conversion")
    public void testJsonRawStringConversion() {
        RecordConverter converter = new JsonRawStringRecordConverter();
        assertAll("",
                () -> assertEquals(EXPECTED_BSON_DOC_RAW_1, converter.convert(null, JSON_STRING_1)),
                () -> assertThrows(DataException.class, () -> converter.convert(null,null))
        );
    }

    @Test
    @DisplayName("test avro or (json + schema) conversion (which is handled the same)")
    public void testAvroOrJsonWithSchemaConversion() {
        RecordConverter converter = new AvroJsonSchemafulRecordConverter();
        assertAll("",
                () -> assertEquals(EXPECTED_BSON_DOC_BYTES_1, converter.convert(OBJ_SCHEMA_1, OBJ_STRUCT_1)),
                () -> assertThrows(DataException.class, () -> converter.convert(OBJ_SCHEMA_1,null)),
                () -> assertThrows(DataException.class, () -> converter.convert(null, OBJ_STRUCT_1)),
                () -> assertThrows(DataException.class, () -> converter.convert(null,null))
        );
    }

    @Test
    @DisplayName("test json object conversion")
    public void testJsonObjectConversion() {
        RecordConverter converter = new JsonSchemalessRecordConverter();
        assertAll("",
                () -> assertEquals(EXPECTED_BSON_DOC_BYTES_1, converter.convert(null, OBJ_MAP_1)),
                () -> assertThrows(DataException.class, () -> converter.convert(null,null))
        );
    }

}
