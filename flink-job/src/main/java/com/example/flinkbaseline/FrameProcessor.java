package com.example.flinkbaseline;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;

import javax.imageio.ImageIO;
import javax.imageio.IIOImage;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class FrameProcessor extends RichMapFunction<FrameMessage, FrameResult> {
    public static final int DEFAULT_REFINEMENT_PASSES = 12;
    private static final float JPEG_QUALITY = 0.85f;
    private final int refinementPasses;

    public FrameProcessor() {
        this(DEFAULT_REFINEMENT_PASSES);
    }

    public FrameProcessor(int refinementPasses) {
        if (refinementPasses < 0) {
            throw new IllegalArgumentException("refinement-passes must be non-negative");
        }
        this.refinementPasses = refinementPasses;
    }

    @Override
    public FrameResult map(FrameMessage frame) {
        FrameResult result = new FrameResult();
        result.frame_id = frame.frame_id;
        result.stream_id = frame.stream_id;
        result.run_id = frame.run_id;
        result.source_sent_at = frame.source_sent_at;
        result.created_at = frame.created_at;
        result.payload_bytes = frame.payload_bytes;
        result.flink_received_at = Instant.now().toString();
        result.processing_started_at = Instant.now().toString();

        BufferedImage image;
        try {
            image = ImageIO.read(new ByteArrayInputStream(frame.payload));
        } catch (IOException | RuntimeException error) {
            return finishError(result, "jpeg_decode_error");
        }
        if (image == null) {
            return finishError(result, "jpeg_decode_error");
        }
        result.decode_finished_at = Instant.now().toString();

        BufferedImage mask;
        try {
            mask = segment(image, refinementPasses);
        } catch (RuntimeException error) {
            return finishError(result, "image_transformation_error");
        }
        result.segmentation_finished_at = Instant.now().toString();
        // Retain the old field for existing consumers, where it means the end
        // of image transformation. It now marks segmentation completion.
        result.transformation_finished_at = result.segmentation_finished_at;

        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            ImageWriter writer = ImageIO.getImageWritersByFormatName("jpeg").next();
            try (ImageOutputStream imageOutput = ImageIO.createImageOutputStream(output)) {
                writer.setOutput(imageOutput);
                ImageWriteParam params = writer.getDefaultWriteParam();
                params.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
                params.setCompressionQuality(JPEG_QUALITY);
                writer.write(null, new IIOImage(mask, null, null), params);
            } finally {
                writer.dispose();
            }
            result.output_payload = output.toByteArray();
            result.output_payload_bytes = result.output_payload.length;
            result.encode_finished_at = Instant.now().toString();
            result.processing_status = "ok";
            result.processing_finished_at = Instant.now().toString();
            return result;
        } catch (IOException | RuntimeException error) {
            return finishError(result, "jpeg_encode_error");
        }
    }

    static BufferedImage segment(BufferedImage image, int refinementPasses) {
        int width = image.getWidth();
        int height = image.getHeight();
        boolean[] leftPadding = new boolean[width];
        boolean[] rightPadding = new boolean[width];
        int left = 0;
        while (left < width && isPaddingColumn(image, left)) {
            leftPadding[left] = true;
            left++;
        }
        int right = width - 1;
        while (right >= left && isPaddingColumn(image, right)) {
            rightPadding[right] = true;
            right--;
        }

        boolean[] seeds = new boolean[width * height];
        int best = -1;
        long bestDistance = Long.MAX_VALUE;
        double centerX = (width - 1) / 2.0;
        double centerY = (height - 1) / 2.0;
        for (int y = 0; y < height; y++) {
            for (int x = left; x <= right; x++) {
                int rgb = image.getRGB(x, y);
                int r = (rgb >> 16) & 0xff;
                int g = (rgb >> 8) & 0xff;
                int b = rgb & 0xff;
                int brightness = (r + g + b) / 3;
                boolean warm = r > b + 10 && r > g + 6 && brightness > 55;
                boolean dark = brightness < 105 && b <= r + 12;
                if (warm || dark) {
                    int index = y * width + x;
                    seeds[index] = true;
                    long distance = Math.round((x - centerX) * (x - centerX)
                        + (y - centerY) * (y - centerY));
                    if (distance < bestDistance) {
                        bestDistance = distance;
                        best = index;
                    }
                }
            }
        }

        boolean[] mask = new boolean[width * height];
        if (best >= 0) {
            floodFill(seeds, mask, best, width, height);
            mask = dilate(mask, width, height);
            mask = dilate(mask, width, height);
            for (int pass = 0; pass < refinementPasses; pass++) {
                mask = refine(mask, width, height);
            }
        }
        for (int x = 0; x < width; x++) {
            if (leftPadding[x] || rightPadding[x]) {
                for (int y = 0; y < height; y++) mask[y * width + x] = false;
            }
        }

        BufferedImage output = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY);
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                output.getRaster().setSample(x, y, 0, mask[y * width + x] ? 255 : 0);
            }
        }
        return output;
    }

    private static boolean isPaddingColumn(BufferedImage image, int x) {
        for (int y = 0; y < image.getHeight(); y += 16) {
            int rgb = image.getRGB(x, y);
            int brightness = (((rgb >> 16) & 0xff) + ((rgb >> 8) & 0xff) + (rgb & 0xff)) / 3;
            if (brightness >= 32) return false;
        }
        return true;
    }

    private static void floodFill(boolean[] seeds, boolean[] mask, int start, int width, int height) {
        int[] queue = new int[seeds.length];
        int head = 0, tail = 0;
        queue[tail++] = start;
        mask[start] = true;
        while (head < tail) {
            int index = queue[head++];
            int x = index % width, y = index / width;
            for (int dy = -1; dy <= 1; dy++) for (int dx = -1; dx <= 1; dx++) {
                if (dx == 0 && dy == 0) continue;
                int nx = x + dx, ny = y + dy;
                if (nx >= 0 && nx < width && ny >= 0 && ny < height) {
                    int next = ny * width + nx;
                    if (seeds[next] && !mask[next]) { mask[next] = true; queue[tail++] = next; }
                }
            }
        }
    }

    private static boolean[] dilate(boolean[] input, int width, int height) {
        boolean[] output = new boolean[input.length];
        for (int y = 0; y < height; y++) for (int x = 0; x < width; x++) {
            int index = y * width + x;
            for (int dy = -1; dy <= 1 && !output[index]; dy++) for (int dx = -1; dx <= 1; dx++) {
                int nx = x + dx, ny = y + dy;
                if (nx >= 0 && nx < width && ny >= 0 && ny < height && input[ny * width + nx]) {
                    output[index] = true; break;
                }
            }
        }
        return output;
    }

    private static boolean[] refine(boolean[] input, int width, int height) {
        boolean[] output = new boolean[input.length];
        for (int y = 0; y < height; y++) for (int x = 0; x < width; x++) {
            int count = 0;
            for (int dy = -2; dy <= 2; dy++) for (int dx = -2; dx <= 2; dx++) {
                int nx = x + dx, ny = y + dy;
                if (nx >= 0 && nx < width && ny >= 0 && ny < height && input[ny * width + nx]) count++;
            }
            output[y * width + x] = count >= 7;
        }
        return output;
    }

    private static FrameResult finishError(FrameResult result, String status) {
        result.processing_status = status;
        result.output_payload = new byte[0];
        result.output_payload_bytes = 0;
        result.processing_finished_at = Instant.now().toString();
        return result;
    }
}
