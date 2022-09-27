package com.kinhnv.app.kafka.connect.transforms;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class DublicateField<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String DUPLICATE_FIELDS = "duplicate.fields";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(DUPLICATE_FIELDS, ConfigDef.Type.LIST, "duplicate_field", ConfigDef.Importance.HIGH,
                    "Field name for duplication");

    private static final String PURPOSE = "adding duplicate field to record";

    private Cache<Schema, Schema> schemaUpdateCache;

    private Map<String, String> fields;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);

        fields = parseRenameMappings(config.getList(DUPLICATE_FIELDS));

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    }

    static Map<String, String> parseRenameMappings(List<String> mappings) {
        final Map<String, String> m = new HashMap<>();
        for (String mapping : mappings) {
            final String[] parts = mapping.split(":");
            if (parts.length == 2) {
                m.put(parts[0], parts[1]);
            }
        }
        return m;
    }

    String field(String fieldName) {
        final String mapping = fields.get(fieldName);
        return mapping == null ? fieldName : mapping;
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

        final Map<String, Object> updatedValue = new HashMap<>(value.size());

        for (Map.Entry<String, Object> e : value.entrySet()) {
            final String fieldName = e.getKey();
            final Object fieldValue = e.getValue();
            updatedValue.put(fieldName, fieldValue);

            for (Map.Entry<String, String> field : fields.entrySet()) {
                if (field.getValue().equals(fieldName)) {
                    final String newfieldName = field.getKey();
                    updatedValue.put(newfieldName, fieldValue);
                }
            }
        }

        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            final String fieldName = field.name();
            final Object fieldValue = value.get(field);
            updatedValue.put(fieldName, fieldValue);

            for (Map.Entry<String, String> fieldsEntity : fields.entrySet()) {
                if (fieldsEntity.getValue().equals(fieldName)) {
                    final String newfieldName = fieldsEntity.getKey();
                    updatedValue.put(newfieldName, fieldValue);
                }
            }
        }

        return newRecord(record, updatedSchema, updatedValue);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field : schema.fields()) {
            builder.field(field(field.name()), field.schema());

            for (Map.Entry<String, String> fieldsEntity : fields.entrySet()) {
                if (fieldsEntity.getValue().equals(field.name())) {
                    final String newfieldName = fieldsEntity.getKey();
                    builder.field(newfieldName, field.schema());
                }
            }
        }

        return builder.build();
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends DublicateField<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue,
                    record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends DublicateField<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                    updatedSchema, updatedValue, record.timestamp());
        }

    }
}
