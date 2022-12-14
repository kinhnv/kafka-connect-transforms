package com.kinhnv.app.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class FixNullFieldTest {

  private FixNullField<SourceRecord> xform = new FixNullField.Value<>();

  @After
  public void tearDown() throws Exception {
    xform.close();
  }

  @Test
  public void copySchemaAndDuplicateField() {
    final Map<String, Object> props = new HashMap<>();
    props.put("field", "magic");
    props.put("value", "{}");

    xform.configure(props);

    final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc")
        .field("magic", Schema.OPTIONAL_INT64_SCHEMA).build();
    final Struct simpleStruct = new Struct(simpleStructSchema).put("magic", 42L);

    final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
    final SourceRecord transformedRecord = xform.apply(record);

    assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
    assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
    assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());

    assertEquals(Schema.OPTIONAL_INT64_SCHEMA, transformedRecord.valueSchema().field("magic").schema());
    assertEquals(42L, ((Struct) transformedRecord.value()).getInt64("magic").longValue());
    assertEquals(Schema.OPTIONAL_INT64_SCHEMA, transformedRecord.valueSchema().field("magic1").schema());
    assertEquals(42L, ((Struct) transformedRecord.value()).getInt64("magic1").longValue());

    // Exercise caching
    final SourceRecord transformedRecord2 = xform.apply(
        new SourceRecord(null, null, "test", 1, simpleStructSchema, new Struct(simpleStructSchema)));
    assertSame(transformedRecord.valueSchema(), transformedRecord2.valueSchema());

  }

  @Test
  public void schemalessDublicateField() {
    final Map<String, Object> props = new HashMap<>();
    props.put("field", "magic");
    props.put("value", "{}");

    xform.configure(props);

    final SourceRecord record = new SourceRecord(null, null, "test", 0,
        null, Collections.singletonMap("magic", null));

    final SourceRecord transformedRecord = xform.apply(record);
    assertNotNull(((Map) transformedRecord.value()).get("magic"));
  }
}
