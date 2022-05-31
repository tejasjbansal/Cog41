import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;



public class Offence {
	public static class MapClass extends Mapper<LongWritable,Text,Text,IntWritable>
	   {
		 private Text Vehicle = new Text();
		 private IntWritable Speed = new IntWritable();
		 
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	    	  
	         try{
	            String[] str = value.toString().split(",");	 
	            int speed = Integer.parseInt(str[1]);
	            Vehicle.set(str[0]);
	            Speed.set(speed);
	            

	            context.write(Vehicle, Speed);
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }

public static class ReduceClass extends Reducer<Text,IntWritable,Text,DoubleWritable>
	   {
		    private DoubleWritable result = new DoubleWritable();
		    
		    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
				double off_count = 0;
				double total_count = 0;
				double off_percent = 0;

				for (IntWritable value : values) {
					if (value.get() > 65)
					{
					  off_count++;	
					}
					total_count++;
				}
				off_percent = (off_count*100)/total_count;
				result.set(off_percent);

		      context.write(key, result);
		      
		      
		    }
	   }
public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    //conf.set("name", "value")
    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
    Job job = Job.getInstance(conf, "Percentage of Offence");
    job.setJarByClass(Offence.class);
    job.setMapperClass(MapClass.class);
    //job.setCombinerClass(ReduceClass.class);
    job.setReducerClass(ReduceClass.class);
    job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}