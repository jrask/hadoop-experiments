package com.jayway.hadoop.fress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;

public class ByteOutputFormat<K, V> extends FileOutputFormat {


    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job

    ) throws IOException, InterruptedException {


        Configuration conf = job.getConfiguration();
        boolean isCompressed = getCompressOutput(job);
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            Class<? extends CompressionCodec> codecClass =
                    getOutputCompressorClass(job, GzipCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
            extension = codec.getDefaultExtension();
        }
        Path file = getDefaultWorkFile(job, extension);
        FileSystem fs = file.getFileSystem(conf);
        if (!isCompressed) {
            FSDataOutputStream fileOut = fs.create(file, false);
            return new ByteRecordWriter<K, V>(fileOut);
        } else {
            FSDataOutputStream fileOut = fs.create(file, false);
            return new ByteRecordWriter<K, V>(new DataOutputStream
                    (codec.createOutputStream(fileOut)));
        }
    }
}


class ByteRecordWriter<K, V> extends RecordWriter<K, V> {
    private DataOutputStream out;

    public ByteRecordWriter(DataOutputStream out) {
        this.out = out;
    }

    public void write(K key, V value) throws IOException {
        boolean nullValue = value == null || value instanceof NullWritable;
        if (!nullValue) {
            BytesWritable bw = (BytesWritable) value;
            out.write(bw.copyBytes());
            out.flush();
        } else {
            out.write(new byte[]{0,1,2,3,4,5,6,7,7,7,5,5,5,5});
        }
    }



    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        out.close();
    }
}
