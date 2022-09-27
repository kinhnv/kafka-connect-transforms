package com.kinhnv.app.kafka.connect.transforms;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.protocol.types.Field.Str;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class WrapField<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String WRAP_FIELD_CONFIG = "wrapField";
    private static final String FIELDS_CONFIG = "fields";
    private static final String PURPOSE = "wrapping field in record";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(WRAP_FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.MEDIUM,
                    "Field name for the single field that will be created in the resulting Struct or Map.")
            .define(FIELDS_CONFIG, ConfigDef.Type.LIST, "", ConfigDef.Importance.MEDIUM,
                    "Field name for the single field that will be created in the resulting Struct or Map.");

    private Cache<Schema, Schema> schemaUpdateCache;

    private String wrapFieldName;
    private List<String> fieldNames;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        wrapFieldName = config.getString(WRAP_FIELD_CONFIG);
        fieldNames = config.getList(FIELDS_CONFIG);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    }

    @Override
    public R apply(R record) {
        final Schema schema = operatingSchema(record);
        if (schema == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Object valueOrigin = operatingValue(record);
        Map<String, Object> updatedValue = new HashMap<>();
        if (fieldNames.size() == 0) {
            updatedValue.put(wrapFieldName, valueOrigin);
        } else if (valueOrigin != null) {

            Map<String, Object> wrapValue = new HashMap<>();

            final Map<String, Object> value = requireMap(valueOrigin, PURPOSE);
            for (Map.Entry<String, Object> entity : value.entrySet()) {
                if (!fieldNames.contains(entity.getKey())) {
                    updatedValue.put(entity.getKey(), entity.getValue());
                } else {
                    wrapValue.put(entity.getKey(), entity.getValue());
                }
            }
            if (wrapValue.size() > 0) {
                updatedValue.put(wrapFieldName, value);
            }
        }
        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Schema schema = operatingSchema(record);
        final Object valueOrigin = operatingValue(record);
        Schema updatedSchema = schemaUpdateCache.get(schema);
        Struct updatedValue = null;

        if (fieldNames.size() == 0) {
            if (updatedSchema == null) {
                updatedSchema = SchemaBuilder.struct().field(wrapFieldName, schema).build();
                schemaUpdateCache.put(schema, updatedSchema);
            }
            updatedValue = new Struct(updatedSchema).put(wrapFieldName, valueOrigin);

        } else if (valueOrigin != null) {
            Struct wrapSchemaValue = null;
            boolean hasWrap = false;
            final Struct value = requireStruct(operatingValue(record), PURPOSE);
            if (updatedSchema == null) {
                final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
                final SchemaBuilder wrapSchemaBuilder = SchemaBuilder.struct();
                for (Field field : schema.fields()) {
                    if (!fieldNames.contains(field.name())) {
                        builder.field(field.name(), field.schema());
                    } else {
                        wrapSchemaBuilder.field(field.name(), field.schema());
                    }
                }
                final Schema wrapSchema = wrapSchemaBuilder.build();
                wrapSchemaValue = new Struct(wrapSchema);
                updatedSchema = builder.field(wrapFieldName, wrapSchema).build();
                schemaUpdateCache.put(schema, updatedSchema);
            }
            updatedValue = new Struct(updatedSchema);
            for (Field field : value.schema().fields()) {
                if (!fieldNames.contains(field.name())) {
                    updatedValue.put(field.name(), value.get(field));
                } else {
                    hasWrap = true;
                    wrapSchemaValue.put(field.name(), value.get(field));
                }
            }
            if (hasWrap) {
                updatedValue.put(wrapFieldName, wrapSchemaValue);
            }
        }

        return newRecord(record, updatedSchema, updatedValue);
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends WrapField<R> {
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

    public static class Value<R extends ConnectRecord<R>> extends WrapField<R> {
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
