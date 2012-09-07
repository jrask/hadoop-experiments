package com.jayway.hadoop.fress;

import com.jayway.hadoop.util.ByteOutputFormat;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.JsonNode;

import java.io.IOException;
import java.util.UUID;

public class ImageGenerator extends Configured implements Tool {




    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = new Job(getConf(), "Fress logs");
        job.setJarByClass(getClass());


        //MultipleOutputs.addNamedOutput(job, "image", ByteOutputFormat.class,
          //      NullWritable.class, BytesWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ImageGeneratorMapper.class);
        //job.setCombinerClass(FressExcludeDuplicateMessagesReducer.class);
        job.setReducerClass(ImageGeneratorReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setOutputFormatClass(ByteOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        System.out.println("***********");

        System.out.println("Is Interface: " + TaskAttemptContext.class.isInterface());
        System.out.println("******");

        int exitCode = ToolRunner.run(new ImageGenerator(), args);
        System.exit(exitCode);
    }
}


class ImageGeneratorMapper extends Mapper<LongWritable, Text, Text,BytesWritable> {

    byte[] imageAsBase64;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //imageAsBase64 = getBase64();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        JsonNode node = JsonUtil.fromText(value);

        try {
            String image = node.get("payload").get("params").get("image").get("encoded").getTextValue();
            System.out.println(node.get("payload").get("id").getTextValue());
            byte bytes[] = Base64.decodeBase64(image.getBytes());
            context.write(new Text(UUID.randomUUID().toString()),new BytesWritable(bytes));
        } catch (NullPointerException e) {
            System.out.println(e.getMessage());
        }
    }
}

class ImageGeneratorReducer extends Reducer< Text, BytesWritable,Text,BytesWritable> {
    MultipleOutputs<Text,BytesWritable> outputs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outputs = new MultipleOutputs<Text, BytesWritable>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;
        for(BytesWritable image: values) {
            count++;
            if(image.getLength() > 0) {
                outputs.write(key, image, "images/image-" + UUID.randomUUID().toString() + ".jpg");
            }
        }
    }
}