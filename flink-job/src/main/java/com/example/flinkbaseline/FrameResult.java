package com.example.flinkbaseline;

public class FrameResult {
    public long frame_id;
    public String stream_id;
    public String run_id;
    public String source_sent_at;
    public String created_at;
    public long payload_bytes;
    public long output_payload_bytes;
    public byte[] output_payload;
    public String flink_received_at;
    public String processing_started_at;
    public String decode_finished_at;
    public String segmentation_finished_at;
    public String transformation_finished_at;
    public String encode_finished_at;
    public String processing_finished_at;
    public String result_written_at;
    public String processing_status;
}
