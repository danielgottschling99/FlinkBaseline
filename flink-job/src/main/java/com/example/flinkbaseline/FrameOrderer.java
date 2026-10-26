package com.example.flinkbaseline;

import java.util.TreeMap;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

public class FrameOrderer extends RichFlatMapFunction<FrameResult, FrameResult> {
    private final long firstFrameId;
    private transient TreeMap<Long, FrameResult> pending;
    private long nextFrameId;

    public FrameOrderer() {
        this(1L);
    }

    public FrameOrderer(long firstFrameId) {
        if (firstFrameId < 0) {
            throw new IllegalArgumentException("first-frame-id must be non-negative");
        }
        this.firstFrameId = firstFrameId;
        this.nextFrameId = firstFrameId;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        pending = new TreeMap<>();
        nextFrameId = firstFrameId;
    }

    @Override
    public void flatMap(FrameResult result, Collector<FrameResult> out) {
        if (result.frame_id < nextFrameId) {
            return; // Ignore a replayed result rather than emitting it twice.
        }
        pending.put(result.frame_id, result);
        while (pending.containsKey(nextFrameId)) {
            out.collect(pending.remove(nextFrameId));
            nextFrameId++;
        }
    }
}
