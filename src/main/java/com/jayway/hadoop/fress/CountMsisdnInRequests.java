package com.jayway.hadoop.fress;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

public class CountMsisdnInRequests extends Configured implements Tool {

    enum ERROR {ERROR}

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

        job.setMapperClass(CountMsisdnInRequestsMapper.class);
        // job.setCombinerClass(ExtractJsonRpcMessagesReducer.class);
        job.setReducerClass(CountMsisdnInRequestReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CountMsisdnInRequests(), args);
        System.exit(exitCode);
    }
}

class CountMsisdnInRequestsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


    ObjectMapper mapper = new ObjectMapper();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        JsonNode node = null;
        try {
            node = mapper.readValue(value.getBytes(), JsonNode.class);


        if (node != null && node.has("payload") && node.get("payload").has("id")) {
            if (node.get("payload").has("params")) {
                JsonNode id = node.get("payload").get("params").get("session").get("id");
                if (id.get("type").getTextValue().equals("msisdn")) {
                    String msisdn = id.get("value").getTextValue();
                    context.write(new Text(msisdn), new IntWritable(1));
                }
            }
        }
        } catch (RuntimeException e) {
            System.out.println(e.getMessage());
            //context.getCounter(CountMsisdnInRequests.ERROR.ERROR).increment(1);
        }
    }
}

class CountMsisdnInRequestReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text msisdn, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int total = 0;
        for (IntWritable value : values) {
            total++;
        }
        context.write(msisdn, new IntWritable(total));
    }
}
