import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class AllTimeLow {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,DoubleWritable>
	   {
		 private Text stock_id = new Text();
		 private DoubleWritable Low = new DoubleWritable();
		 
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	    	  
	         try{
	            String[] str = value.toString().split(",");	 
	            double low = Double.parseDouble(str[5]);
	            stock_id.set(str[1]);
	            Low.set(low);
	            
	            //context.write(new Text(str[1]),new LongWritable(vol));
	            context.write(stock_id, Low);
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	  public static class ReduceClass extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
	   {
		    private DoubleWritable result = new DoubleWritable();
		    
		    public void reduce(Text key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
				double minValue=Double.MAX_VALUE;
				double temp_val=0;
				
				for (DoubleWritable value : values) {
					temp_val = value.get();
					if (temp_val < minValue) {
						minValue = temp_val;
					}
				}
				result.set(minValue);

		      context.write(key, result);
		      //context.write(key, new LongWritable(sum));
		      
		    }
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job = Job.getInstance(conf, "Highest Price for each stock");
		    job.setJarByClass(AllTimeLow.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    job.setNumReduceTasks(1);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(DoubleWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}