package com.example.flinkbaseline;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FrameDeserializer implements KafkaRecordDeserializationSchema<FrameMessage> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<FrameMessage> out) throws IOException {
        FrameMessage frame = new FrameMessage();
        frame.frame_id = requiredLong(record, "frame_id");
        frame.stream_id = required(record, "stream_id");
        frame.run_id = required(record, "run_id");
        frame.source_sent_at = required(record, "source_sent_at");
        frame.created_at = frame.source_sent_at;
        frame.payload_bytes = record.value() == null ? 0 : record.value().length;
        frame.payload = record.value();
        out.collect(frame);
    }

    @Override
    public TypeInformation<FrameMessage> getProducedType() {
        return TypeInformation.of(new TypeHint<>() {});
    }

    private static String required(ConsumerRecord<byte[], byte[]> record, String name) throws IOException {
        byte[] value = header(record, name);
        if (value == null) throw new IOException("Missing Kafka header " + name);
        return new String(value, StandardCharsets.UTF_8);
    }

    private static long requiredLong(ConsumerRecord<byte[], byte[]> record, String name) throws IOException {
        try { return Long.parseLong(required(record, name)); }
        catch (NumberFormatException error) { throw new IOException("Invalid Kafka header " + name, error); }
    }

    private static byte[] header(ConsumerRecord<byte[], byte[]> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null ? null : header.value();
    }
}
