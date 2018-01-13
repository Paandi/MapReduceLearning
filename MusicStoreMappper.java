import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;

class IntArrayWritable extends ArrayWritable
{
	public IntArrayWritable()
	{
		super(IntWritable.class);
	}
	
	public IntArrayWritable(IntWritable i[])
	{
		super(IntWritable.class);
		set(i);
	}
}



public class MusicStoreMappper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntArrayWritable> 
{

	public void map(LongWritable key, Text value, OutputCollector<Text, IntArrayWritable> op, Reporter reporter) throws IOException 
	{
		
		StringTokenizer str=new StringTokenizer(value.toString(),"|");
		str.nextToken();
		
		Text k=new Text(str.nextToken());
		
		IntArrayWritable aw=new IntArrayWritable(new IntWritable[] {new IntWritable(Integer.parseInt(str.nextToken())), new IntWritable(Integer.parseInt(str.nextToken())), new IntWritable(Integer.parseInt(str.nextToken()))   });
		op.collect(k, aw);
		
	}

}

