package at.grahsl.kafka.connect.mongodb;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static avro.shaded.com.google.common.collect.Lists.partition;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

@RunWith(JUnitPlatform.class)
public class MongoDbSinkRecordBatchesTest {

    private static List<List<SinkRecord>> LIST_INITIAL_EMPTY = new ArrayList<>();
    private static final int NUM_FAKE_RECORDS = 50;

    @BeforeAll
    static void setupVerificationList() {
        LIST_INITIAL_EMPTY.add(new ArrayList<>());
    }

    @TestFactory
    @DisplayName("test batching with different config params for max.batch.size")
    Stream<DynamicTest> testBatchingWithDifferentConfigsForBatchSize() {

        return Stream.iterate(0, r -> r + 1).limit(NUM_FAKE_RECORDS+1)
                .map(batchSize -> dynamicTest("test batching for "
                                                    +NUM_FAKE_RECORDS+" records with batchsize="+batchSize, () -> {
                    MongoDbSinkRecordBatches batches = new MongoDbSinkRecordBatches(batchSize, NUM_FAKE_RECORDS);
                    assertEquals(LIST_INITIAL_EMPTY, batches.getBufferedBatches());
                    List<SinkRecord> recordList = createSinkRecordList("foo",0,0,NUM_FAKE_RECORDS);
                    recordList.forEach(batches::buffer);
                    List<List<SinkRecord>> batchedList = partition(recordList, batchSize > 0 ? batchSize : recordList.size());
                    assertEquals(batchedList, batches.getBufferedBatches());
                }));

    }

    private static List<SinkRecord> createSinkRecordList(String topic, int partition, int beginOffset, int size) {
        List<SinkRecord> list = new ArrayList<>();
        for(int i = 0; i < size; i++) {
            list.add(new SinkRecord(topic,partition,null,null,null,null, beginOffset+i));
        }
        return list;
    }




}
