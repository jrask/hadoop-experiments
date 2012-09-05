package com.jayway.hadoop.fress;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class ExcludeDuplicateMessagesDriver extends Configured implements Tool {
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

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ExcludeDuplicateMessagesMapper.class);
        // job.setCombinerClass(ExtractJsonRpcMessagesReducer.class);
        job.setReducerClass(ExcludeDuplicateMessagesReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ExcludeDuplicateMessagesDriver(), args);
        System.exit(exitCode);
    }

}

class ExcludeDuplicateMessagesMapper extends Mapper<LongWritable, Text, Text, Text> {


    ObjectMapper mapper = new ObjectMapper();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        JsonNode node = null;
        try {
            node = mapper.readValue(value.getBytes(), JsonNode.class);
        } catch (RuntimeException e) {
            System.out.println(e.getMessage());
        }


        if (node != null && node.has("payload") && node.get("payload").has("id")) {
            if(node.get("payload").has("params")) {
                context.write(new Text("IN" + node.get("payload").get("id").getTextValue()), value);
            } else if(node.get("payload").has("result")) {
                context.write(new Text("OUT" + node.get("payload").get("id").getTextValue()), value);
            }
        }
    }
}


class ExcludeDuplicateMessagesReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(values.iterator().next(), new Text(""));
    }
}
