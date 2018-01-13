import java.util.Iterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


public class MusicStoreReducer extends MapReduceBase implements Reducer<Text, IntArrayWritable, Text, Text> {
	
	public void reduce(Text key, Iterator<IntArrayWritable> value, OutputCollector<Text, Text> op, Reporter reporter )
	{   
		
	     try{
	    		
	 		int listen=0;
	 		int radio=0;
	 		int skip=0;
	     
			while(value.hasNext())
			{
		
				System.out.println(key);
			    
				IntArrayWritable ax=value.next();	
                
				Writable[] x=ax.get();    
				//IntWritable[] x=(IntWritable[]) ax.get();
				
			    System.out.println("class of c = " + x.length);

			    listen=listen+((IntWritable) x[0]).get();
			    skip=skip+((IntWritable) x[1]).get();
			    radio=radio+((IntWritable) x[2]).get();
	       	
			// 	op.collect(key, new Text("pandi"));
			}
			
		    String str="Song Id: "+key+ " \nlisten  "+listen + " times, skipped "+skip + " times, radio "+ radio+" times";
	        
		    op.collect(key, new Text(str));	
	
	     }
	     catch(Exception e)
	     {
	    	 e.printStackTrace();
	     }
	}
}



