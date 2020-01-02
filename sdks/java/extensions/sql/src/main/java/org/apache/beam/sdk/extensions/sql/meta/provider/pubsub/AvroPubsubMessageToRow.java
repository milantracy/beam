/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sql.meta.provider.pubsub;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Instant;

/** Read side converter for {@link PubsubMessage} with Avro payload. */
@Internal
@Experimental
@AutoValue
public abstract class AvroPubsubMessageToRow extends PubsubMessageToRow implements Serializable {

  @Override
  public PCollectionTuple expand(PCollection<PubsubMessage> input) {
    PCollectionTuple rows =
        input.apply(
            ParDo.of(new FlatSchemaPubsubMessageToRow(messageSchema(), useDlq()))
                .withOutputTags(
                    MAIN_TAG, useDlq() ? TupleTagList.of(DLQ_TAG) : TupleTagList.empty()));
    return rows;
  }

  public static Builder builder() {
    return new AutoValue_AvroPubsubMessageToRow.Builder();
  }

  @Internal
  private static class FlatSchemaPubsubMessageToRow extends DoFn<PubsubMessage, Row> {

    private final Schema messageSchema;

    private final boolean useDlq;

    protected FlatSchemaPubsubMessageToRow(Schema messageSchema, boolean useDlq) {
      this.messageSchema = messageSchema;
      this.useDlq = useDlq;
    }

    private GenericRecord parsePayload(PubsubMessage pubsubMessage) {
      byte[] avroPayload = pubsubMessage.getPayload();

      // Construct payload flat schema.
      Schema payloadSchema =
          new Schema(
              messageSchema.getFields().stream()
                  .filter(field -> !TIMESTAMP_FIELD.equals(field.getName()))
                  .collect(Collectors.toList()));
      org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(payloadSchema);
      return AvroUtils.toGenericRecord(avroPayload, avroSchema);
    }

    private Object getValuedForFieldFlatSchema(Field field, Instant timestamp, Row payload) {
      String fieldName = field.getName();
      if (TIMESTAMP_FIELD.equals(fieldName)) {
        return timestamp;
      } else {
        return payload.getValue(fieldName);
      }
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      try {
        GenericRecord record = parsePayload(context.element());
        System.out.println(record);
        Row row = AvroUtils.toBeamRowStrict(record, null);
        List<Object> values =
            messageSchema.getFields().stream()
                .map(field -> getValuedForFieldFlatSchema(field, context.timestamp(), row))
                .collect(Collectors.toList());
        context.output(Row.withSchema(messageSchema).addValues(values).build());
      } catch (Exception exception) {
        if (useDlq) {
          context.output(DLQ_TAG, context.element());
        } else {
          throw new RuntimeException("Error parsing message", exception);
        }
      }
    }
  }

  @Internal
  private static class NestedSchemaPubsubMessageToRow extends DoFn<PubsubMessage, Row> {

    private final Schema messageSchema;

    private final boolean useDlq;

    protected NestedSchemaPubsubMessageToRow(Schema messageSchema, boolean useDlq) {
      this.messageSchema = messageSchema;
      this.useDlq = useDlq;
    }

    private GenericRecord parsePayload(PubsubMessage pubsubMessage) {
      byte[] avroPayload = pubsubMessage.getPayload();
      Schema payloadSchema = messageSchema.getField(PAYLOAD_FIELD).getType().getRowSchema();
      org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(payloadSchema);
      return AvroUtils.toGenericRecord(avroPayload, avroSchema);
    }

    private Object getValueForField(
        Field field, Instant timestamp, Map<String, String> attributes, Row payload) {
      switch (field.getName()) {
        case TIMESTAMP_FIELD:
          return timestamp;
        case ATTRIBUTES_FIELD:
          return attributes;
        case PAYLOAD_FIELD:
          return payload;
        default:
          throw new IllegalArgumentException(
              "Unexpected field '"
                  + field.getName()
                  + "' in top level schema"
                  + " for Pubsub message. Top level schema should only contain "
                  + "'timestamp', 'attributes', and 'payload' fields");
      }
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      try {
        Row row = AvroUtils.toBeamRowStrict(parsePayload(context.element()), null);
        List<Object> values =
            messageSchema.getFields().stream()
                .map(
                    field ->
                        getValueForField(
                            field, context.timestamp(), context.element().getAttributeMap(), row))
                .collect(Collectors.toList());
        context.output(Row.withSchema(messageSchema).addValues(values).build());
      } catch (Exception exception) {
        if (useDlq) {
          context.output(DLQ_TAG, context.element());
        } else {
          throw new RuntimeException("Error parsing message", exception);
        }
      }
    }
  }

  @Override
  public boolean useFlatSchema() {
    return true;
  }

  @AutoValue.Builder
  abstract static class Builder {
    public abstract Builder messageSchema(Schema messageSchema);

    public abstract Builder useDlq(boolean useDlq);

    public abstract AvroPubsubMessageToRow build();
  }
}
