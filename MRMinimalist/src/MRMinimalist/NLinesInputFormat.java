
package MRMinimalist;

//import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
        


public class NLinesInputFormat extends TextInputFormat{
    @Override
    
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new NLinesRecordReader();
    }
    
   // protected boolean isSplitable(JobContext context, Path file) {
   //return false;
   //}
    
}