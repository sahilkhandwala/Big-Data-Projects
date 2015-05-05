import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.util.LineReader;

public class MultiFileReader {
	
	public static class MultiFileWritable implements WritableComparable<MultiFileWritable> {

		private long offset;
		private String fileName;

		@Override
		public void readFields(DataInput in) throws IOException {
			this.offset = in.readLong();
			this.fileName = Text.readString(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(offset);
			Text.writeString(out, fileName);
		}
		@Override
		public int compareTo(MultiFileWritable o) {
			MultiFileWritable that = (MultiFileWritable)o;

			int f = this.fileName.compareTo(that.fileName);
			if(f == 0) {
				return (int)Math.signum((double)(this.offset - that.offset));
			}
			return f;
		}
		@Override
		public boolean equals(Object obj) {
			if(obj instanceof MultiFileWritable)
				return this.compareTo((MultiFileWritable) obj) == 0;
			return false;
		}
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((fileName == null) ? 0 : fileName.hashCode());
			result = prime * result + (int) (offset ^ (offset >>> 32));
			return result;
		}
	}


	public static class CombineFileLineRecordReader extends RecordReader<MultiFileWritable, Text> {

		private long startOffset; //offset of the chunk;
		private long end; //end of the chunk;
		private long pos; // current pos 
		private FileSystem fs;
		private Path path;
		private MultiFileWritable key;
		private Text value;

		private FSDataInputStream fileIn;
		private LineReader reader;

		public CombineFileLineRecordReader(CombineFileSplit split,
				TaskAttemptContext context, Integer index) throws IOException {

			this.path = split.getPath(index);
			fs = this.path.getFileSystem(context.getConfiguration());
			this.startOffset = split.getOffset(index);
			this.end = startOffset + split.getLength(index);
			boolean skipFirstLine = false;

			//open the file
			fileIn = fs.open(path);
			if (startOffset != 0) {
				skipFirstLine = true;
				--startOffset;
				fileIn.seek(startOffset);
			}
			reader = new LineReader(fileIn);
			if (skipFirstLine) {  // skip first line and re-establish "startOffset".
				startOffset += reader.readLine(new Text(), 0,
						(int)Math.min((long)Integer.MAX_VALUE, end - startOffset));
			}
			this.pos = startOffset;
		}

		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
		}

		public void close() throws IOException { }

		public float getProgress() throws IOException {
			if (startOffset == end) {
				return 0.0f;
			} else {
				return Math.min(1.0f, (pos - startOffset) / (float)(end - startOffset));
			}
		}

		public boolean nextKeyValue() throws IOException {
			if (key == null) {
				key = new MultiFileWritable();
				key.fileName = path.getName();
			}
			key.offset = pos;
			if (value == null) {
				value = new Text();
			}
			int newSize = 0;
			if (pos < end) {
				newSize = reader.readLine(value);
				pos += newSize;
			}
			if (newSize == 0) {
				key = null;
				value = null;
				return false;
			} else {
				return true;
			}
		}

		public MultiFileWritable getCurrentKey() 
				throws IOException, InterruptedException {
			return key;
		}

		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}
	}

	public static class MyInputFormat extends CombineFileInputFormat<MultiFileWritable, Text>  {

		public MyInputFormat(){
			super();
			setMaxSplitSize(67108864); // 64 MB, default block size on hadoop
		}	

		public RecordReader<MultiFileWritable,Text> createRecordReader(InputSplit split,
				TaskAttemptContext context) throws IOException {
			return new CombineFileRecordReader<MultiFileWritable, Text>(
					(CombineFileSplit)split, context, CombineFileLineRecordReader.class);
		}
		
		@Override
		  protected boolean isSplitable(JobContext context, Path file){
		    return false;
		 }
	}
}