package at.grahsl.kafka.connect.mongodb.cdc.debezium;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@RunWith(JUnitPlatform.class)
public class OperationTypeTest {

    @Test
    @DisplayName("when op type 'c' then type CREATE")
    public void testOperationTypeCreate() {
        assertEquals(OperationType.CREATE,OperationType.fromText("c"));
    }

    @Test
    @DisplayName("when op type 'r' then type READ")
    public void testOperationTypeRead() {
        assertEquals(OperationType.READ,OperationType.fromText("r"));
    }

    @Test
    @DisplayName("when op type 'u' then type UPDATE")
    public void testOperationTypeUpdate() {
        assertEquals(OperationType.UPDATE,OperationType.fromText("u"));
    }

    @Test
    @DisplayName("when op type 'd' then type DELETE")
    public void testOperationTypeDelete() {
        assertEquals(OperationType.DELETE,OperationType.fromText("d"));
    }

    @Test
    @DisplayName("when invalid op type IllegalArgumentException")
    public void testOperationTypeInvalid() {
        assertThrows(IllegalArgumentException.class,
                () -> OperationType.fromText("x"));
    }

}
