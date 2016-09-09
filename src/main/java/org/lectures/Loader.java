package org.lectures;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Loader extends Configured implements Tool {
    private String zkConnectString;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Loader(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        int result = parseArgs(args);
        if (result != 0) {
            return result;
        }

        Configuration config = getConf();

        Job job = new Job(config, "warc-extraction");
        job.setJarByClass(Loader.class);

        job.setMapperClass(WarcMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setInputFormatClass(FilenameInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path("fhb-warcs"));
        
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error executing job!");
        }

        return 0;
    }

    @SuppressWarnings("static-access")
    protected int parseArgs(String[] args) {
        Options cliOptions = new Options();

        Option zkOption = OptionBuilder
                .isRequired()
                .withArgName("connection-string")
                .hasArg()
                .withDescription("ZooKeeper connection string: hostname1:port,hostname2:port,...")
                .withLongOpt("zookeeper")
                .create("z");
        cliOptions.addOption(zkOption);

        CommandLineParser parser = new PosixParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(cliOptions, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            System.out.println();

            HelpFormatter help = new HelpFormatter();
            help.printHelp(getClass().getSimpleName(), cliOptions, true);
            return 1;
        }

        zkConnectString = cmd.getOptionValue(zkOption.getOpt());

        return 0;
    }
}
