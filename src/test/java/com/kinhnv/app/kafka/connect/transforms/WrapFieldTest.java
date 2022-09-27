package com.kinhnv.app.kafka.connect.transforms;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class WrapFieldTest {
    private final WrapField<SinkRecord> xform = new WrapField.Key<>();

    @After
    public void teardown() {
        xform.close();
    }

    @Test
    public void schemalessWithoutFields() {
        xform.configure(Collections.singletonMap("wrapField", "magic"));

        final SinkRecord record = new SinkRecord("test", 0, null, 42, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertNull(transformedRecord.keySchema());
        assertEquals(Collections.singletonMap("magic", 42), transformedRecord.key());
    }

    @Test
    public void schemalessWithFields() {
        final Map<String, Object> props = new HashMap<>();

        props.put("wrapField", "wrapField");
        props.put("fields", "magic");
        xform.configure(props);

        final Map<String, Object> value = new HashMap<>();

        value.put("magic", 42L);
        value.put("name", "magic");

        final SinkRecord record = new SinkRecord("test", 0, null, value, null, null,
                0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertNull(transformedRecord.keySchema());
        assertEquals(42L, ((Map) ((Map) transformedRecord.key()).get("wrapField")).get("magic"));
        assertEquals("magic", ((Map) transformedRecord.key()).get("name"));
    }

    @Test
    public void withSchemaWithoutFields() {
        xform.configure(Collections.singletonMap("wrapField", "magic"));

        final SinkRecord record = new SinkRecord("test", 0, Schema.INT32_SCHEMA, 42, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertEquals(Schema.Type.STRUCT, transformedRecord.keySchema().type());
        assertEquals(record.keySchema(), transformedRecord.keySchema().field("magic").schema());
        assertEquals(42, ((Struct) transformedRecord.key()).get("magic"));
    }

    @Test
    public void withSchemaWithFields() {
        final Map<String, Object> props = new HashMap<>();

        props.put("wrapField", "wrapField");
        props.put("fields", "magic");

        xform.configure(props);

        final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc")
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("magic", Schema.OPTIONAL_INT64_SCHEMA).build();
        final Struct simpleStruct = new Struct(simpleStructSchema).put("magic", 42L).put("name", "magic");

        final SinkRecord record = new SinkRecord("test", 0, simpleStructSchema, simpleStruct, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertEquals(Schema.Type.STRUCT, transformedRecord.keySchema().type());
        assertEquals(record.keySchema(), transformedRecord.keySchema().field("magic").schema());
        assertEquals(42L, ((Struct) ((Struct) transformedRecord.key()).get("wrapField")).get("magic"));
    }

    @Test
    public void withSchemaWithFieldsAndMultipleFields() {
        final Map<String, Object> props = new HashMap<>();

        props.put("wrapField", "wrapField");
        props.put("fields", "magic,name");

        xform.configure(props);

        final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc")
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("magic", Schema.OPTIONAL_INT64_SCHEMA).build();
        final Struct simpleStruct = new Struct(simpleStructSchema).put("magic", 42L).put("name", "magic");

        final SinkRecord record = new SinkRecord("test", 0, simpleStructSchema, simpleStruct, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertEquals(Schema.Type.STRUCT, transformedRecord.keySchema().type());
        assertEquals(record.keySchema(), transformedRecord.keySchema().field("magic").schema());
        assertEquals(42L, ((Struct) ((Struct) transformedRecord.key()).get("wrapField")).get("magic"));
    }

}
