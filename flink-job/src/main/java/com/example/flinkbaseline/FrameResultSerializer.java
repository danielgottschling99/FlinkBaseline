package com.example.flinkbaseline;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

public class FrameResultSerializer implements KafkaRecordSerializationSchema<FrameResult> {
    private final String topic;

    public FrameResultSerializer(String topic) { this.topic = topic; }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(FrameResult result, KafkaRecordSerializationSchema.KafkaSinkContext context, Long timestamp) {
        result.result_written_at = Instant.now().toString();
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, result.output_payload);
        header(record, "frame_id", Long.toString(result.frame_id));
        header(record, "stream_id", result.stream_id);
        header(record, "run_id", result.run_id);
        header(record, "source_sent_at", result.source_sent_at);
        header(record, "payload_bytes", Long.toString(result.payload_bytes));
        header(record, "output_payload_bytes", Long.toString(result.output_payload_bytes));
        header(record, "flink_received_at", result.flink_received_at);
        header(record, "processing_started_at", result.processing_started_at);
        header(record, "decode_finished_at", result.decode_finished_at);
        header(record, "segmentation_finished_at", result.segmentation_finished_at);
        header(record, "transformation_finished_at", result.transformation_finished_at);
        header(record, "encode_finished_at", result.encode_finished_at);
        header(record, "processing_finished_at", result.processing_finished_at);
        header(record, "result_written_at", result.result_written_at);
        header(record, "processing_status", result.processing_status);
        return record;
    }

    private static void header(ProducerRecord<byte[], byte[]> record, String name, String value) {
        record.headers().add(name, (value == null ? "" : value).getBytes(StandardCharsets.UTF_8));
    }
}
