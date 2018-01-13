import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;

class MusicStoreDriver extends Configured implements Tool
{
    public int run(String args[]) throws IOException
    {
    	
    	JobConf job= new JobConf(getConf(),MusicStoreDriver.class);
    	
    	job.setJobName("MusicStoreDriver");
    	
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);
    	
    	job.setMapOutputKeyClass(Text.class);
    	job.setMapOutputValueClass(IntArrayWritable.class);
    	
    	job.setMapperClass(MusicStoreMappper.class);
    	job.setReducerClass(MusicStoreReducer.class);

    	
    	FileInputFormat.addInputPath(job,new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	
    	JobClient.runJob(job);
    	
    	return 0;
    }
    
    
    public static void main(String args[]) throws Exception
    {
    	int i=ToolRunner.run(new Configuration(),new MusicStoreDriver(),args);
        System.exit(i);
    }
	
}
