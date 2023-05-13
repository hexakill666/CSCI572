package hw3;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HW3 {
    public static void main(String[] args) throws Exception{
        String unigramJobName = "unigram";
        Configuration unigramJobConf = new Configuration();
        Class unigramJobJarByClass = HW3.class;
        Class unigramJobMapperClass = UnigramMapper.class;
        Class unigramJobCombinerClass = MultigramReducer.class;
        Class unigramJobReducerClass = MultigramReducer.class;
        Class unigramJobOutputKeyClass = Text.class;
        Class unigramJobOutputValueClass = Text.class;
        List<String> unigramInputPathList = new ArrayList<>(){{
            add("my_input/fulldata");
        }};
        List<String> unigramOutputList = new ArrayList<>(){{
            add("my_output/" + unigramJobName);
        }};

        String bigramJobName = "bigram";
        Configuration bigramJobConf = new Configuration();
        Class bigramJobJarByClass = HW3.class;
        Class bigramJobMapperClass = BigramMapper.class;
        Class bigramJobCombinerClass = MultigramReducer.class;
        Class bigramJobReducerClass = MultigramReducer.class;
        Class bigramJobOutputKeyClass = Text.class;
        Class bigramJobOutputValueClass = Text.class;
        List<String> bigramInputPathList = new ArrayList<>(){{
            add("my_input/devdata");
        }};
        List<String> bigramOutputList = new ArrayList<>(){{
            add("my_output/" + bigramJobName);
        }};

        System.out.println("==========" + unigramJobName + " start==========");
        runJob(
            unigramJobName, 
            unigramJobConf, 
            unigramJobJarByClass, 
            unigramJobMapperClass, 
            unigramJobCombinerClass, 
            unigramJobReducerClass, 
            unigramJobOutputKeyClass, 
            unigramJobOutputValueClass, 
            unigramInputPathList, 
            unigramOutputList
        );
        System.out.println("==========" + unigramJobName + " finish==========");

        System.out.println("==========" + bigramJobName + " start==========");
        runJob(
            bigramJobName, 
            bigramJobConf, 
            bigramJobJarByClass, 
            bigramJobMapperClass, 
            bigramJobCombinerClass, 
            bigramJobReducerClass, 
            bigramJobOutputKeyClass, 
            bigramJobOutputValueClass, 
            bigramInputPathList, 
            bigramOutputList
        );
        System.out.println("==========" + bigramJobName + " finish==========");
    }

    public static void runJob(
        String jobName, 
        Configuration jobConf, 
        Class jobJarByClass,
        Class jobMapperClass,
        Class jobCombinerClass,
        Class jobReducerClass,
        Class jobOutputKeyClass,
        Class jobOutputValueClass,
        List<String> inputPathList,
        List<String> outputPathList
    ) throws Exception
    {
        Job job = Job.getInstance(jobConf, jobName);

        job.setJarByClass(jobJarByClass);
        job.setMapperClass(jobMapperClass);
        // job.setCombinerClass(jobCombinerClass);
        job.setReducerClass(jobReducerClass);

        job.setOutputKeyClass(jobOutputKeyClass);
        job.setOutputValueClass(jobOutputValueClass);

        for(String inputPath : inputPathList){
            FileInputFormat.addInputPath(job, new Path(inputPath));
        }
        for(String outputPath : outputPathList){
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
        }

        job.waitForCompletion(true);
    }
}
