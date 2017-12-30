package at.grahsl.kafka.connect.mongodb.cdc.debezium;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import static org.junit.jupiter.api.Assertions.*;

@RunWith(JUnitPlatform.class)
public class OperationTypeTest {

    @Test
    @DisplayName("when op type 'c' then type CREATE")
    public void testOperationTypeCreate() {
        String textType = "c";
        OperationType otCreate = OperationType.fromText(textType);
        assertAll(
                () -> assertEquals(OperationType.CREATE,otCreate),
                () -> assertEquals(textType,otCreate.type())
        );
    }

    @Test
    @DisplayName("when op type 'r' then type READ")
    public void testOperationTypeRead() {
        String textType = "r";
        OperationType otRead = OperationType.fromText(textType);
        assertAll(
                () -> assertEquals(OperationType.READ,otRead),
                () -> assertEquals(textType,otRead.type())
        );
    }

    @Test
    @DisplayName("when op type 'u' then type UPDATE")
    public void testOperationTypeUpdate() {
        String textType = "u";
        OperationType otUpdate = OperationType.fromText(textType);
        assertAll(
                () -> assertEquals(OperationType.UPDATE,otUpdate),
                () -> assertEquals(textType,otUpdate.type())
        );
    }

    @Test
    @DisplayName("when op type 'd' then type DELETE")
    public void testOperationTypeDelete() {
        String textType = "d";
        OperationType otDelete = OperationType.fromText(textType);
        assertAll(
                () -> assertEquals(OperationType.DELETE,otDelete),
                () -> assertEquals(textType,otDelete.type())
        );
    }

    @Test
    @DisplayName("when invalid op type IllegalArgumentException")
    public void testOperationTypeInvalid() {
        assertThrows(IllegalArgumentException.class,
                () -> OperationType.fromText("x"));
    }

}
