import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AccessAnalysis {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, Text>{
	private Text userID_spName = new Text();
    private Text access_traffic = new Text();//�����ʴ�������������Text��
    private int traffic=0;
    private int one=1;
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	if(Integer.valueOf(key.toString()) == 0)	
            return;
        StringTokenizer st = new StringTokenizer(value.toString());
        String[] strArray=new String[8];
        int i=0;
    	while(st.hasMoreTokens()){         //��ÿ��value���ַ����������ʽ���뷽���ȡ
    		strArray[i++]=st.nextToken();
    	}
    	traffic=Integer.valueOf(strArray[6])+Integer.valueOf(strArray[7]);//�������к����������ܺ�
    	userID_spName.set(strArray[2]+","+strArray[5]);//���û�������վspName��Ϊkeyֵ  	
    	access_traffic.set(String.valueOf(one)+","+String.valueOf(traffic));//�Է��ʴ���1����������������Ϊvalueֵ
    	context.write(userID_spName, access_traffic);//д��ÿ��key_value��
    }  
    
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,Text,Text,Text> {
    private Text Access_Traffic = new Text();

    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int access = 0;
      int traffic=0;
      for (Text val : values) {
        String[] access_traffic= val.toString().split(",");//��ÿ��valueֵ�����ַ��������з����ȡ
        access+=Integer.valueOf(access_traffic[0]);//����ÿ��key�ķ��ʴ����ܺ�
        traffic+=Integer.valueOf(access_traffic[1]);//����ÿ��key�������ܺ�
      }
      Access_Traffic.set(String.valueOf(access)+","+String.valueOf(traffic));
      context.write(key, Access_Traffic);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: traffic <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "AccessAnalysis");
    job.setJarByClass(AccessAnalysis.class);
    job.setMapperClass(TokenizerMapper.class);
   // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
