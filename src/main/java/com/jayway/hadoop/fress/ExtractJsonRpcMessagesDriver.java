package com.jayway.hadoop.fress;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

public class ExtractJsonRpcMessagesDriver extends Configured implements Tool {
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

        job.setMapperClass(ExtractJsonRpcMessagesMapper.class);
        job.setCombinerClass(ExtractJsonRpcMessagesReducer.class);
        job.setReducerClass(ExtractJsonRpcMessagesReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ExtractJsonRpcMessagesDriver(), args);
        System.exit(exitCode);
    }

}

class ExtractJsonRpcMessagesMapper extends Mapper<LongWritable, Text, Text, NullWritable> {


    ObjectMapper mapper = new ObjectMapper();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        int start =  value.find("{\"payload\":");
        if(start != -1) {
            String s = value.toString();

            s = s.substring(start,s.lastIndexOf("}") + 1);


            JsonNode node = null;
            try {
                node = mapper.readValue(s,JsonNode.class);
            } catch(RuntimeException e) {
                System.out.println(e.getMessage());

            }

            if(node != null && node.has("payload") && node.get("payload").has("id")) {
                context.write(new Text(mapper.writeValueAsString(node)),NullWritable.get());
            }

            //context.write(new Text(s),NullWritable.get());
        }
    }
}

class ExtractJsonRpcMessagesReducer extends Reducer<Text, NullWritable, Text, NullWritable> {


    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
