
package MRMinimalist;
import java.lang.Math;
import java.util.Collections;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapred.JobConf;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
//import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit; 
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapred.JobClient;


public class MRMinimalist {


    public static class GroupSortMapper 
       extends Mapper<Object, Text, Text, Text>{
    
     //private final static IntWritable one = new IntWritable(1);
    private Text TKey = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      /*StringTokenizer itr = new StringTokenizer(value.toString());
      
      FileSplit fileSplit = (FileSplit)context.getInputSplit();
      String filename = fileSplit.getPath().getName();
      System.out.println("File name "+filename);
      System.out.println("Directory and File name"+fileSplit.getPath().toString());
      
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);*/
        
        //String line = value.toString();
        //String []lineArr = lines.split("\n");
        //int lcount = lineArr.length;
        TKey.set("Step1");
        String T =value.toString().trim().replace(" ", "");
        if (T.equalsIgnoreCase("")==false){
        
            
            context.write(TKey,value);}
 
        
      }
    }
  
  
///////////////////////////////////////////////////////////////
  public static class GroupSortReducer 
       extends Reducer<Text,Text,NullWritable,Text> {
    //private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      
      int Groups=0;  
      Configuration conf = context.getConfiguration();
      Groups=conf.getInt("NOGroups", 1);  
      int x =0;  
      int Total = 0;
      String S="";
      NullWritable N=null;
      //for (Text val : values) {
        //Total++;
      //}
      
      ArrayList<Points> cache = new ArrayList<Points>();
      
      for (Text val : values) {
               
                //Text writable = new Text();
                //writable.set(val.toString());
                //cache.add(writable);
          
          String line = val.toString();
          String []lineArr = line.split(" ");
        //int lcount = lineArr.length;
          Points TPoints=new Points();
          
          TPoints.x=Double.valueOf(lineArr[0]);
          TPoints.Record=line.trim();
          cache.add(TPoints);
          
          
            }
      Collections.sort(cache);
     
      Total=cache.size(); // // Note this approch does not work for odd sizes of the input datasets
      
      if ((Total%2)==0){
      for (int i=0; i<Groups;i++){
      
          for (int j=0; j<(Total/Groups);j++){
          
          S=i+" ";
         
          S=S+cache.get(x).Record;
          S.replace("\n", "");
          Text T=new Text ();
          T.set(S);
          //cache.set(x,S); 
          
          context.write(N,T);
          
          x++;
          }
      }
      
      while (x<cache.size()){
          S=(Groups-1)+" ";         
          S=S+cache.get(x).Record;
          S.replace("\n", "");
          Text T=new Text ();
          T.set(S);         
          context.write(N,T);
          x++;   
    
      
      
      }
      
      
      
      }
      else
      {
         
          for (int i=0; i<Groups;i++){
      
          for (int j=0; j<(Total/Groups);j++){
          
          S=i+" ";
         
          S=S+cache.get(x).Record;
          S.replace("\n", "");
          Text T=new Text ();
          T.set(S);
          //cache.set(x,S); 
          
          context.write(N,T);
          
          x++;
          }
            
          }
      
        while (x<cache.size()){
          S=(Groups-1)+" ";         
          S=S+cache.get(x).Record;
          S.replace("\n", "");
          Text T=new Text ();
          T.set(S);         
          context.write(N,T);
          x++;   
    
      
      
      }
      
      
      
      
           
    }
  }
  }
 
///////////////////////////////////////////////////////////////  
  public static class MainPartitions 
       extends Mapper<Object, Text, Text, Text>{
    
     //private final static IntWritable one = new IntWritable(1);
    private Text TKey = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      /*StringTokenizer itr = new StringTokenizer(value.toString());
      
      FileSplit fileSplit = (FileSplit)context.getInputSplit();
      String filename = fileSplit.getPath().getName();
      System.out.println("File name "+filename);
      System.out.println("Directory and File name"+fileSplit.getPath().toString());
      
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);*/
        
        String line = value.toString();
        String []lineArr = line.split(" ");
        //int lcount = lineArr.length;
        TKey.set(lineArr[0]);
        context.write(TKey,value);
 
        
      }
    }
  
//////////////////////////////////////////////////////////////  
 
  public static class MainConvexHulls 
       extends Reducer<Text,Text,NullWritable,Text> {
    //private IntWritable result = new IntWritable();

    static int NIL = 0;
    static double INF = 1e30;
     
 /*     
    static void act(int point) {
    if ( P[ P[point].prev ].next != point) {   // insert
        P[ P[point].prev ].next = P[ P[point].next ].prev = point;
    }
    else { // delete
        P[ P[point].prev ].next = P[point].next;
        P[ P[point].next ].prev = P[point].prev;
    }
}  */    
      
    static void ConvexHull3D(Point P[], int A[],int B[], int n,int Lindex, int Rindex)
{
   
        
	if (n ==1){  return ;} 
		

	int mid=Lindex+n/2;
				
	ConvexHull3D(P,B,A,n/2,Lindex,mid-1);  
	ConvexHull3D(P,B,A,n-n/2,mid,Rindex);
		
			
	int leftGroupIndex=mid-1; 
	int rightGroupIndex = (leftGroupIndex +1);
       
	int eventListOffset =Lindex*2;
        int point;      

        // u: end of left hull based on x coordinate
        // v: beginning of right hull based on x coordinate
        int u = leftGroupIndex;
        int v = rightGroupIndex;
        
	int i = Lindex*2;
        int j = i+n/2*2;
        
		  

        // find end of list for u
        for ( ; P[u].next != NIL; u = P[u].next) ;
       
        // FIND INITIAL BRIDGE for u and v
        for ( ; ; ) {
                int p, q, r;
                // calculate turn1 value
                double turn1;
                p = u; q = v; r = P[v].next;
                if (p == NIL || q == NIL || r == NIL){ turn1 = 1.0;}
            else
                {    turn1 = (P[q].x - P[p].x)*(P[r].y - P[p].y) - (P[r].x - P[p].x)*(P[q].y - P[p].y);}

                // calculate turn2 value
                double turn2;
                p = P[u].prev; q = u; r = v;
                if  (p == NIL || q == NIL || r == NIL) { turn2 = 1.0; }
            else
                {    turn2 = (P[q].x - P[p].x)*(P[r].y - P[p].y) - (P[r].x - P[p].x)*(P[q].y - P[p].y);}

                if (turn1 < 0)  {v = P[v].next;}
                
                else
                    if (turn2 < 0)  { u = P[u].prev;}
                else
                {
                    break;}
        }

        int k = eventListOffset;    // starting index of event list
        double oldTime = -INF;       // initialize oldTime to negative inifinity
        double newTime;
        double [] t=new double[6];                 // array to hold 6 times for each set of time calculations
        int minimumTime=6;

        // TRACK BRIDGE FROM U TO V OVER TIME
        // progress through time in an infinite loop until
        // no more insertion/deletion events occur
        for ( ; ; oldTime = newTime) {          
                // calculate each moment in time: (if time is < 0 , then clockwise)
                int p, q, r;

                // calculate time 0 value
                // t[0] = time(P[ B[i] ].prev, B[i], P[ B[i] ].next);   // B[i]
                
                p = P[ B[i] ].prev; q = B[i]; r = P[ B[i] ].next;
                if (p == NIL || q == NIL || r == NIL) { t[0] = INF;}
            else
                {t[0] = ((P[q].x - P[p].x)*(P[r].z - P[p].z) - (P[r].x - P[p].x)*(P[q].z - P[p].z))/((P[q].x - P[p].x)*(P[r].y - P[p].y) - (P[r].x - P[p].x)*(P[q].y - P[p].y));}

                // calculate time 1 value
                // t[1] = time(P[ B[j] ].prev, B[j], P[ B[j] ].next);   // B[j]
                
                p = P[ B[j] ].prev; q = B[j]; r = P[ B[j] ].next;
                if (p == NIL || q == NIL || r == NIL) { t[1] = INF; }
            else
                {t[1] = ((P[q].x - P[p].x)*(P[r].z - P[p].z) - (P[r].x - P[p].x)*(P[q].z - P[p].z))/((P[q].x - P[p].x)*(P[r].y - P[p].y) - (P[r].x - P[p].x)*(P[q].y - P[p].y));}

                // calculate time 2 value
                // t[2] = time(P[u].prev, u, v);                        // -u u v
                p = P[u].prev; q = u; r = v;
                if (p == NIL || q == NIL || r == NIL) { t[2] = INF;}
            else
                { t[2] = ((P[q].x - P[p].x)*(P[r].z - P[p].z) - (P[r].x - P[p].x)*(P[q].z - P[p].z))/((P[q].x - P[p].x)*(P[r].y - P[p].y) - (P[r].x - P[p].x)*(P[q].y - P[p].y));}

                // calculate time 3 value
                // t[3] = time(u, P[u].next, v);                        // u +u v
                p = u; q = P[u].next; r = v;
                if (p == NIL || q == NIL || r == NIL) { t[3] = INF;}
            else
                {t[3] = ((P[q].x - P[p].x)*(P[r].z - P[p].z) - (P[r].x - P[p].x)*(P[q].z - P[p].z))/((P[q].x - P[p].x)*(P[r].y - P[p].y) - (P[r].x - P[p].x)*(P[q].y - P[p].y));}

                // calculate time 4 value
                // t[4] = time(u, P[v].prev, v);                        // u -v v
                p = u; q = P[v].prev; r = v;
                if (p == NIL || q == NIL || r == NIL) { t[4] = INF;}
            else
                {t[4] = ((P[q].x - P[p].x)*(P[r].z - P[p].z) - (P[r].x - P[p].x)*(P[q].z - P[p].z))/((P[q].x - P[p].x)*(P[r].y - P[p].y) - (P[r].x - P[p].x)*(P[q].y - P[p].y));}

        // calculate time 5 value
                // t[5] = time(u, v, P[v].next);                        // u v v+
                p = u; q = v; r = P[v].next;
                if (p == NIL || q == NIL || r == NIL) { t[5] = INF;}
            else
                { t[5] = ((P[q].x - P[p].x)*(P[r].z - P[p].z) - (P[r].x - P[p].x)*(P[q].z - P[p].z))/((P[q].x - P[p].x)*(P[r].y - P[p].y) - (P[r].x - P[p].x)*(P[q].y - P[p].y));}

                newTime = INF;  // initialize newTime to no new events in time

                // find the scenario with the minimum moment in time calculation
                for (int x=0; x<6; x++) {
                        if (t[x] > oldTime && t[x] < newTime) {
                                minimumTime = x;
                                newTime = t[x];
                        }
                }

                // check to see if no new events occured
                if (newTime == INF)
                { 
                    break; } // break out of infinite moment in time loop

                // new events have occured, thus, act on the
                // event with the lowest time calculation
                switch (minimumTime) {
                        case 0:
                               if (P[ B[i] ].x < P[u].x) { A[k++] = B[i];}      	  
							    
                                // ACT
                                point = B[i++];             
								
                                if ( P[ P[point].prev ].next != point) {   // insert
                                        P[ P[point].prev ].next = P[ P[point].next ].prev = point;
                                }
                                else { // delete
                                        P[ P[point].prev ].next = P[point].next;
                                        P[ P[point].next ].prev = P[point].prev;
                                }
                                // END ACT
                                break;
                        case 1:
                                if (P[ B[j] ].x > P[v].x) { A[k++] = B[j];}
                                
                                // ACT
                                point = B[j++];
                                if ( P[ P[point].prev ].next != point) {   // insert
                                        P[ P[point].prev ].next = P[ P[point].next ].prev = point;
                                }
                                else { // delete
                                        P[ P[point].prev ].next = P[point].next;
                                        P[ P[point].next ].prev = P[point].prev;
                                }
                                // END ACT
                                break;
                        case 2: // -u u v
                                A[k++] = u;
                                u = P[u].prev;
								
                                break;
                        case 3: // u +u v
                                u = P[u].next;
                                A[k++] = u;
								
                                break;
                        case 4: // u -v v
                                v = P[v].prev;
                                A[k++] = v;
								
                                break;
                        case 5: // u v +v
                                A[k++] = v;
                                v = P[v].next;
								
                                break;
                }
        }

        A[k] = NIL;

        // connect both groups
        P[u].next = v;
        P[v].prev = u;

		for (k--; k >= eventListOffset; k--) {
		
                if (P[ A[k] ].x <= P[ u ].x || P[ A[k] ].x >= P[ v ].x) {
                        // pass current point to act funtion
                        // ACT
                        point = A[k];
                        if ( P[ P[point].prev ].next != point) {   // insert                                        
                            P[ P[point].prev ].next = P[ P[point].next ].prev = point;
                        }
                        else { // delete
                                P[ P[point].prev ].next = P[point].next;
                                P[ P[point].next ].prev = P[point].prev;
                        }                      
                        // END ACT
                        // get new u or v pointers
                        if (A[k] == u)
                          {
                              u = P[u].prev;}
                        else if (A[k] == v)
                        {
                            v = P[v].next;}
                }
                else {
                        P[u].next = A[k];
                        P[ A[k] ].prev = u;
                        P[v].prev = A[k];
                        P[ A[k] ].next = v;

                        // get new u or v pointers
                        if ( P[ A[k] ].x < P[rightGroupIndex].x )
                        {
                            u = A[k];}
                        else
                        {
                            v = A[k];}
                }
        }



}


    
    
    
    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      NullWritable N=null;
        
      ArrayList<Points> cache = new ArrayList<Points>();
      for (Text val : values) {    
          //cache.add(val.toString().trim());
          if ((val.toString().equalsIgnoreCase("\n")==false)&&(val.toString().equalsIgnoreCase(" ")==false)){
          String line = val.toString();
          String []lineArr = line.split(" ");
        //int lcount = lineArr.length;
          Points TPoints=new Points();
          
          TPoints.x=Double.valueOf(lineArr[1]);
          TPoints.Record=line.trim();
          cache.add(TPoints);
// context.write(N,new Text("This is test "+line));         
          }
          
             
            }
     
      Collections.sort(cache);
        
      int n=cache.size();                     // number of points  // start the array at index 1
      Point [] P =new Point[n+1];             // list of all points
   
      int [] A=new int[(n+1)*2];              // Main list of events
      int [] B=new int[(n+1)*2];              // Temp list of events  
        
        
        int i=1;
        Point TPoint0=new Point ();
        P[0]=TPoint0;
        
        for (int x=0;x<cache.size();x++){
       
            
             String line = cache.get(x).Record;
             String []lineArr = line.split(" ");
             Point TPoint=new Point ();
            TPoint.x=Double.valueOf(lineArr[1]);
             TPoint.y=Double.valueOf(lineArr[2]);
             TPoint.z=Double.valueOf(lineArr[3]);
             TPoint.prev=NIL;
             TPoint.next=NIL;
             P[i++]=TPoint;
    
    // context.write(N,new Text("This is test 2 "+lineArr[1]+">>"+lineArr[2]+">>"+lineArr[3]));         
             
        }
        
       ConvexHull3D(P,A,B,n,1,n-1);  //starting the array points from index 1
        
        
        for (i =2; A[i] != NIL; i++) {
	
            Text OutputRecord=new Text();
            OutputRecord.set(key +" "+Integer.toString(i-2)+" "+((P[ A[i] ].prev)) + " " + (A[i]) + " " + ((P[ A[i] ].next)));
            context.write(N,OutputRecord);
            //System.out.println( "Face " + i + ":     " + ((P[ A[i] ].prev)-1) + " " + (A[i]-1) + " " + ((P[ A[i] ].next)-1));
            //act( A[i] );
            
      
    } 
        Text EndText=new Text(key+" "+"End");
        context.write(N,EndText);
        
        
        for (i =1; i <= n; i++) {
	
            Text OutputRecord=new Text();
            OutputRecord.set(key+" "+Integer.toString(i-1)+" "+(P[i].x)+" "+(P[i].y)+" "+(P[i].z)+" "+(P[i].prev) + " "+(P[i].next));
            context.write(N,OutputRecord);
            //System.out.println( "Face " + i + ":     " + ((P[ A[i] ].prev)-1) + " " + (A[i]-1) + " " + ((P[ A[i] ].next)-1));
            //act( A[i] );
            
      
    } 
     
        //Text EndText=new Text(key+" "+"End");
        context.write(N,EndText);
        
       
        
        
  }
  }
  
//////////////////////////////////////////////////////////////
  

  public static class MergePartitionsMapper 
       extends Mapper<Object, Text, Text, Text>{
    
     //private final static IntWritable one = new IntWritable(1);
    private Text TKey = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    
        Configuration conf = context.getConfiguration();
        int Depth=Integer.valueOf(conf.get("Depth"));
        int Groups=conf.getInt("NOGroups", 1);
        int IncValue=(int)(Math.pow(2.0,Double.valueOf(String.valueOf(Depth))));
        int NoKeys=Groups/IncValue;
        int x=0;
        Text T=new Text();
        String Temp="";
        
        ArrayList<String> Keys = new ArrayList<String>();
        
        for (int i=0;i<NoKeys;i++){
        
            Keys.add(String.valueOf(x));
            x+=IncValue;
             
        }
        
        if (Depth!=0){
          //  Text Ttt=new Text("Depth>>"+Depth +"+>>Groups>>+ "+Groups+">> IncValue>>"+IncValue+">>NoKeys>>"+NoKeys);
          //   context.write(TKey,Ttt);
          //  for (int i=0;i<Keys.size();i++){
          //  Text Tt=new Text("Keys [i]>>"+ i +"+>>keys>>+ "+Keys.get(i));
          //   context.write(TKey,Tt);
          //  }
            
        for (int i=1;i<Keys.size();i=i+2){
         
         String line = value.toString().trim();
         String []lineArr = line.split(" ");
        // Text Tt=new Text("linearr [0]>>"+lineArr[0]+"+Keys.get(i)+ "+Keys.get(i));
        // context.write(TKey,Tt);
         
         if ((lineArr[0].equalsIgnoreCase(Keys.get(i))==true)&&(lineArr[0].equalsIgnoreCase("0")==false)){
         
             Temp=line.substring(2);
              
             Temp=(Integer.valueOf(lineArr[0])-IncValue)+" "+"C"+" "+Temp;
             break;
            // Text T=new Text(Temp);
            // context.write(TKey,T);
         
         
         }
         
         else{
         
             Temp=line.substring(2);
              
             Temp=lineArr[0]+" "+"B"+" "+Temp;
             //break; 
             // Text T=new Text(Temp);
            // context.write(TKey,T);
         
         
         }
         
        
        }
        
        }
        else{
        
        if (Depth==0){
        
         String line = value.toString().trim();
         String []lineArr = line.split(" ");
         if (Integer.valueOf(lineArr[0])%2!=0){
         
             Temp=line.substring(2);
              
             Temp=Integer.valueOf(lineArr[0])-1+" "+"C"+" "+Temp;
              
             // Text T=new Text(Temp);
            // context.write(TKey,T);
         
         
         
         
         }
         else{
         
         Temp=line.substring(2);
              
             Temp=lineArr[0]+" "+"B"+" "+Temp;
            // Text T=new Text(Temp);
            // context.write(TKey,T);
         
         
         
         
         }
        
        
        }
        
        
        
        }        
        
        T.set(Temp);
        context.write(TKey,T);
         
        
       // TKey.set("Step1");
       // context.write(TKey,value);
 
        
      }
    }
  
  public static class MergePartitionsReducer 
       extends Reducer<Text,Text,NullWritable,Text> {
    //private IntWritable result = new IntWritable();
    
      
    private boolean find(ArrayList<String> Keys, String Key){
    
        for (int i=0;i<Keys.size();i++){
    
            if (Key.equalsIgnoreCase(Keys.get(i))) return true;
    }
    
    return false;
    } 
      
    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      
     // ArrayList<String> Keys = new ArrayList<String>();
     // ArrayList<String> Indexes = new ArrayList<String>();
      NullWritable N=null;
     
      
    //  ArrayList<String> cache = new ArrayList<String>();
      
   //   int index=0;
      for (Text val : values) {
     //        cache.add(val.toString().trim());
       //      String line = val.toString().trim();
       //      String []lineArr = line.split(" ");
             
       ///      if ((find(Keys,lineArr[0])==false)&&(lineArr[0].equalsIgnoreCase("End")==false)){
       //      Keys.add(lineArr[0]);
       //      Indexes.add(Integer.toString(index));
       //      
             
       //      }
 
       //      index++;
       //     }
      
     // int j=0;
      
    //  for (int i=1;i<Keys.size();i+=2){
          
    //      j= Integer.valueOf(Indexes.get(i));
     //     String TKey="";
          ///int z=0;
       //   do{
         //     if (j<cache.size()){
      //        String []RecordArray=cache.get(j).split(" ");
      //        TKey=RecordArray[0];
             
               //Text T=new Text ("_____________________________");
               //context.write(N,T);
               //String S=Keys.get(i)+"--**--"+TKey;
               //T.set(S);
               //context.write(N,T);
              
        //      if(TKey.equalsIgnoreCase(Keys.get(i))==true){
              
        //      String Temp=cache.get(j).substring(2);
              //Temp=(Integer.valueOf(TKey)-1)+" "+Temp;
         //     Temp=(Keys.get(i-1))+" "+"C"+" "+Temp;
         //     cache.set(j, Temp);         
            //  z++;
              
         //    }
                              
                      
        ///      j++;}      
       //   }while((TKey.equalsIgnoreCase(Keys.get(i))==true)&&(j<cache.size()));
          
          
      
      
    //  }
      
      //////////////////////////////////////////
      
     // j=0;
      
    //  for (int i=0;i<Keys.size();i+=2){
      //int z=0;          
      //    for (j= Integer.valueOf(Indexes.get(i));j<Integer.valueOf(Indexes.get(i+1));j++){
          
        //      String Temp=cache.get(j).substring(2);
              //Temp=(Integer.valueOf(TKey)-1)+" "+Temp;
          //    Temp=(Keys.get(i))+" "+"B"+" "+Temp;
           //   cache.set(j, Temp);         
        //      z++;
          
          
        //  }
                          
        context.write(N,val);  
      
      
      }
      
      
      //////////////////////////////////////////
      
     
     // for (int i=0;i<cache.size();i++){
      
       //    Text T=new Text (cache.get(i));
         //  context.write(N,T);
      
      
      //}
      /*
       Text T=new Text ("_____________________________");
           context.write(N,T);
      
       for (int i=0;i<Keys.size();i++){
      
           String S=Keys.get(i)+"----"+Indexes.get(i);
           T.set(S);
           context.write(N,T);
      
      
      }    
           
      */
      
      
           
    //}
  }

  }
  
  
  
//////////////////////////////////////////////////////////////
  
  
  public static class MergeHullsMapper 
       extends Mapper<Object, Text, Text, Text>{
    
     //private final static IntWritable one = new IntWritable(1);
    private Text TKey = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    
        
        Configuration conf = context.getConfiguration();
        int Depth=Integer.valueOf(conf.get("Depth"));
        int Groups=conf.getInt("NOGroups", 1);
        int IncValue=(int)(Math.pow(2.0,Double.valueOf(String.valueOf(Depth))));
        int NoKeys=Groups/IncValue;
        int x=0;
        Text T=new Text();
        String Temp="";
        
        ArrayList<String> Keys = new ArrayList<String>();
        
        for (int i=0;i<NoKeys;i++){
        
            Keys.add(String.valueOf(x));
            x+=IncValue;
             
        }
        
        if (Depth!=0){
          //  Text Ttt=new Text("Depth>>"+Depth +"+>>Groups>>+ "+Groups+">> IncValue>>"+IncValue+">>NoKeys>>"+NoKeys);
          //   context.write(TKey,Ttt);
          //  for (int i=0;i<Keys.size();i++){
          //  Text Tt=new Text("Keys [i]>>"+ i +"+>>keys>>+ "+Keys.get(i));
          //   context.write(TKey,Tt);
          //  }
            
        for (int i=1;i<Keys.size();i=i+2){
         
         String line = value.toString().trim();
         String []lineArr = line.split(" ");
        // Text Tt=new Text("linearr [0]>>"+lineArr[0]+"+Keys.get(i)+ "+Keys.get(i));
        // context.write(TKey,Tt);
         
         if ((lineArr[0].equalsIgnoreCase(Keys.get(i))==true)&&(lineArr[0].equalsIgnoreCase("0")==false)){
         
             Temp=line.substring(2);
              
             Temp=(Integer.valueOf(lineArr[0])-IncValue)+" "+"C"+" "+Temp;
             TKey.set(String.valueOf((Integer.valueOf(lineArr[0])-IncValue)));
             break;
            // Text T=new Text(Temp);
            // context.write(TKey,T);
         
         
         }
         
         else{
         
             Temp=line.substring(2);
              
             Temp=lineArr[0]+" "+"B"+" "+Temp;
             TKey.set(lineArr[0]);
             //break; 
             // Text T=new Text(Temp);
            // context.write(TKey,T);
         
         
         }
         
        
        }
        
        }
        else{
        
        if (Depth==0){
        
         String line = value.toString().trim();
         String []lineArr = line.split(" ");
         if (Integer.valueOf(lineArr[0])%2!=0){
         
             Temp=line.substring(2);
              
             Temp=Integer.valueOf(lineArr[0])-1+" "+"C"+" "+Temp;
             TKey.set(String.valueOf(Integer.valueOf(lineArr[0])-1)); 
             // Text T=new Text(Temp);
            // context.write(TKey,T);
         
         
         
         
         }
         else{
         
         Temp=line.substring(2);
              
             Temp=lineArr[0]+" "+"B"+" "+Temp;
             TKey.set(lineArr[0]);
            // Text T=new Text(Temp);
            // context.write(TKey,T);
         
         
         
         
         }
        
        
        }
        
        
        
        }        
        
        T.set(Temp);
        context.write(TKey,T);
         
        
       // TKey.set("Step1");
       // context.write(TKey,value);
 
        
      
        
        
        
        
        
        
        //String line = value.toString();
        //String []lineArr = line.split(" ");
        //int lcount = lineArr.length;
        //TKey.set(lineArr[0]);
        //context.write(TKey,value);
        
      }
    }
  
  public static class MergeHullsReducer 
       extends Reducer<Text,Text,NullWritable,Text> {
    //private IntWritable result = new IntWritable();
    
    static int NIL = 0;
    static double INF = 1e30;
       
      
    static void ConvexHull3D(Point P[], int A[],int B[],int C[], int n,int Lindex, int Rindex)
{
   
        
	//if (n ==1){  return ;} 
		

	int mid=Lindex+n/2;
				
	//ConvexHull3D(P,B,A,n/2,Lindex,mid-1);  
	//ConvexHull3D(P,B,A,n-n/2,mid,Rindex);
		
			
	int leftGroupIndex=mid-1; 
	int rightGroupIndex = (leftGroupIndex +1);
       
	//int eventListOffset =Lindex*2;
        int eventListOffset =0;
        int point;      

        // u: end of left hull based on x coordinate
        // v: beginning of right hull based on x coordinate
        int u = leftGroupIndex;
        int v = rightGroupIndex;
        
        
        //int i = Lindex*2;
        //int j = i+n/2*2;
        int i=0;
        int j=0;
		  

        // find end of list for u
        for ( ; P[u].next != NIL; u = P[u].next) ;
       
        // FIND INITIAL BRIDGE for u and v
        for ( ; ; ) {
                int p, q, r;
                // calculate turn1 value
                double turn1;
                p = u; q = v; r = P[v].next;
                if (p == NIL || q == NIL || r == NIL){ turn1 = 1.0;}
            else
                {    turn1 = (P[q].x - P[p].x)*(P[r].y - P[p].y) - (P[r].x - P[p].x)*(P[q].y - P[p].y);}

                // calculate turn2 value
                double turn2;
                p = P[u].prev; q = u; r = v;
                if  (p == NIL || q == NIL || r == NIL) { turn2 = 1.0; }
            else
                {    turn2 = (P[q].x - P[p].x)*(P[r].y - P[p].y) - (P[r].x - P[p].x)*(P[q].y - P[p].y);}

                if (turn1 < 0)  {v = P[v].next;}
                
                else
                    if (turn2 < 0)  { u = P[u].prev;}
                else
                {
                    break;}
        }

        int k = eventListOffset;    // starting index of event list
        double oldTime = -INF;       // initialize oldTime to negative inifinity
        double newTime;
        double [] t=new double[6];                 // array to hold 6 times for each set of time calculations
        int minimumTime=6;

        // TRACK BRIDGE FROM U TO V OVER TIME
        // progress through time in an infinite loop until
        // no more insertion/deletion events occur
        for ( ; ; oldTime = newTime) {          
                // calculate each moment in time: (if time is < 0 , then clockwise)
                int p, q, r;

                // calculate time 0 value
                // t[0] = time(P[ B[i] ].prev, B[i], P[ B[i] ].next);   // B[i]
                
                p = P[ B[i] ].prev; q = B[i]; r = P[ B[i] ].next;
                if (p == NIL || q == NIL || r == NIL) { t[0] = INF;}
            else
                {t[0] = ((P[q].x - P[p].x)*(P[r].z - P[p].z) - (P[r].x - P[p].x)*(P[q].z - P[p].z))/((P[q].x - P[p].x)*(P[r].y - P[p].y) - (P[r].x - P[p].x)*(P[q].y - P[p].y));}

                // calculate time 1 value
                // t[1] = time(P[ B[j] ].prev, B[j], P[ B[j] ].next);   // B[j]
                
                p = P[ C[j] ].prev; q = C[j]; r = P[ C[j] ].next;
                if (p == NIL || q == NIL || r == NIL) { t[1] = INF; }
            else
                {t[1] = ((P[q].x - P[p].x)*(P[r].z - P[p].z) - (P[r].x - P[p].x)*(P[q].z - P[p].z))/((P[q].x - P[p].x)*(P[r].y - P[p].y) - (P[r].x - P[p].x)*(P[q].y - P[p].y));}

                // calculate time 2 value
                // t[2] = time(P[u].prev, u, v);                        // -u u v
                p = P[u].prev; q = u; r = v;
                if (p == NIL || q == NIL || r == NIL) { t[2] = INF;}
            else
                { t[2] = ((P[q].x - P[p].x)*(P[r].z - P[p].z) - (P[r].x - P[p].x)*(P[q].z - P[p].z))/((P[q].x - P[p].x)*(P[r].y - P[p].y) - (P[r].x - P[p].x)*(P[q].y - P[p].y));}

                // calculate time 3 value
                // t[3] = time(u, P[u].next, v);                        // u +u v
                p = u; q = P[u].next; r = v;
                if (p == NIL || q == NIL || r == NIL) { t[3] = INF;}
            else
                {t[3] = ((P[q].x - P[p].x)*(P[r].z - P[p].z) - (P[r].x - P[p].x)*(P[q].z - P[p].z))/((P[q].x - P[p].x)*(P[r].y - P[p].y) - (P[r].x - P[p].x)*(P[q].y - P[p].y));}

                // calculate time 4 value
                // t[4] = time(u, P[v].prev, v);                        // u -v v
                p = u; q = P[v].prev; r = v;
                if (p == NIL || q == NIL || r == NIL) { t[4] = INF;}
            else
                {t[4] = ((P[q].x - P[p].x)*(P[r].z - P[p].z) - (P[r].x - P[p].x)*(P[q].z - P[p].z))/((P[q].x - P[p].x)*(P[r].y - P[p].y) - (P[r].x - P[p].x)*(P[q].y - P[p].y));}

        // calculate time 5 value
                // t[5] = time(u, v, P[v].next);                        // u v v+
                p = u; q = v; r = P[v].next;
                if (p == NIL || q == NIL || r == NIL) { t[5] = INF;}
            else
                { t[5] = ((P[q].x - P[p].x)*(P[r].z - P[p].z) - (P[r].x - P[p].x)*(P[q].z - P[p].z))/((P[q].x - P[p].x)*(P[r].y - P[p].y) - (P[r].x - P[p].x)*(P[q].y - P[p].y));}

                newTime = INF;  // initialize newTime to no new events in time

                // find the scenario with the minimum moment in time calculation
                for (int x=0; x<6; x++) {
                        if (t[x] > oldTime && t[x] < newTime) {
                                minimumTime = x;
                                newTime = t[x];
                        }
                }

                // check to see if no new events occured
                if (newTime == INF)
                { 
                    break; } // break out of infinite moment in time loop

                // new events have occured, thus, act on the
                // event with the lowest time calculation
                switch (minimumTime) {
                        case 0:
                               if (P[ B[i] ].x < P[u].x) { A[k++] = B[i];}      	  
							    
                                // ACT
                                point = B[i++];             
								
                                if ( P[ P[point].prev ].next != point) {   // insert
                                        P[ P[point].prev ].next = P[ P[point].next ].prev = point;
                                }
                                else { // delete
                                        P[ P[point].prev ].next = P[point].next;
                                        P[ P[point].next ].prev = P[point].prev;
                                }
                                // END ACT
                                break;
                        case 1:
                                if (P[ C[j] ].x > P[v].x) { A[k++] = C[j];}
                                
                                // ACT
                                point = C[j++];
                                if ( P[ P[point].prev ].next != point) {   // insert
                                        P[ P[point].prev ].next = P[ P[point].next ].prev = point;
                                }
                                else { // delete
                                        P[ P[point].prev ].next = P[point].next;
                                        P[ P[point].next ].prev = P[point].prev;
                                }
                                // END ACT
                                break;
                        case 2: // -u u v
                                A[k++] = u;
                                u = P[u].prev;
								
                                break;
                        case 3: // u +u v
                                u = P[u].next;
                                A[k++] = u;
								
                                break;
                        case 4: // u -v v
                                v = P[v].prev;
                                A[k++] = v;
								
                                break;
                        case 5: // u v +v
                                A[k++] = v;
                                v = P[v].next;
								
                                break;
                }
        }

        A[k] = NIL;

        // connect both groups
        P[u].next = v;
        P[v].prev = u;

		for (k--; k >= eventListOffset; k--) {
		
                if (P[ A[k] ].x <= P[ u ].x || P[ A[k] ].x >= P[ v ].x) {
                        // pass current point to act funtion
                        // ACT
                        point = A[k];
                        if ( P[ P[point].prev ].next != point) {   // insert                                        
                            P[ P[point].prev ].next = P[ P[point].next ].prev = point;
                        }
                        else { // delete
                                P[ P[point].prev ].next = P[point].next;
                                P[ P[point].next ].prev = P[point].prev;
                        }                      
                        // END ACT
                        // get new u or v pointers
                        if (A[k] == u)
                          {
                              u = P[u].prev;}
                        else if (A[k] == v)
                        {
                            v = P[v].next;}
                }
                else {
                        P[u].next = A[k];
                        P[ A[k] ].prev = u;
                        P[v].prev = A[k];
                        P[ A[k] ].next = v;

                        // get new u or v pointers
                        if ( P[ A[k] ].x < P[rightGroupIndex].x )
                        {
                            u = A[k];}
                        else
                        {
                            v = A[k];}
                }
        }



}


      
    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      
     
      NullWritable N=null;
     
      int nb,nc,n1,n2;
      nb=0;
      n1=0;
      nc=0;
      n2=0;
     
      ArrayList<String> TempB = new ArrayList<String>();
      ArrayList<String> TempC = new ArrayList<String>();
   
      for (Text val : values) {
             
          
          
             
              String line = val.toString().trim();
              String []lineArr = line.split(" ");
            
          
              
             if (lineArr[1].equalsIgnoreCase("B")==true){
              
                 
                  TempB.add(line);
                        if (lineArr.length==6){ nb++; }
                        else { if (lineArr.length>6){ n1++; } }
             }
             else
             if (lineArr[1].equalsIgnoreCase("C")==true){
             
             TempC.add(line);
                        if (lineArr.length==6){ nc++; }
                        else { if (lineArr.length>6){ n2++; } }
             }
             
             }
      
      Point []P=new Point[n1+n2+1];
      int []B=new int[nb+1];
      int []C=new int[nc+1];
      int []A=new int[(n1+n2)*2];
      Point TPoint0=new Point ();
      P[0]=TPoint0;
 
      for (int i=0;i<TempB.size();i++){
              String line = TempB.get(i);
              String []lineArr = line.split(" ");
              if (lineArr.length==6){
                       
              int index=Integer.valueOf(lineArr[2]);
              B[index]=(Integer.valueOf(lineArr[4]));
              
              }
              else
                  if(lineArr.length>6){ 
                  
                  int index=Integer.valueOf(lineArr[2])+1;
                  Point TempPoint=new Point();
                  TempPoint.x=Double.valueOf(lineArr[3]);
                  TempPoint.y=Double.valueOf(lineArr[4]);
                  TempPoint.z=Double.valueOf(lineArr[5]);
                  TempPoint.prev=(Integer.valueOf(lineArr[6]));
                
                  TempPoint.next=(Integer.valueOf(lineArr[7]));
               
                  P[index]=TempPoint;
                  
                  
                  }
      
      
      
      }
      
            for (int i=0;i<TempC.size();i++){
              String line = TempC.get(i);
              String []lineArr = line.split(" ");
              if (lineArr.length==6){
                       
              int index=Integer.valueOf(lineArr[2]);
              C[index]=(Integer.valueOf(lineArr[4]))+n1;
              
              }
              else
                  if(lineArr.length>6){ 
                  
                  int index=(Integer.valueOf(lineArr[2]))+n1+1;
                  Point TempPoint=new Point();
                  TempPoint.x=Double.valueOf(lineArr[3]);
                  TempPoint.y=Double.valueOf(lineArr[4]);
                  TempPoint.z=Double.valueOf(lineArr[5]);
                  
                  if (Integer.valueOf(lineArr[6])!=0){
                      TempPoint.prev=(Integer.valueOf(lineArr[6]))+n1;}
                  else{ TempPoint.prev=(Integer.valueOf(lineArr[6]));} 
                  
                  
                  
                  if (Integer.valueOf(lineArr[7])!=0){
                      TempPoint.next=(Integer.valueOf(lineArr[7]))+n1;}
                  else{ TempPoint.next=(Integer.valueOf(lineArr[7]));} 
                  
          
                  P[index]=TempPoint;
                  
                  
                  }
      
      
      
      }
      
            ConvexHull3D(P,A,B,C,P.length-1,1,P.length-1);  //starting the array points from index 1
            
            for (int i =0; A[i] != NIL; i++) {
	
            Text OutputRecord=new Text();
            OutputRecord.set(key +" "+Integer.toString(i)+" "+((P[ A[i] ].prev)) + " " + (A[i]) + " " + ((P[ A[i] ].next)));
            context.write(N,OutputRecord);
           
            
      
    } 
        Text EndText=new Text(key+" "+"End");
        context.write(N,EndText);
        
        
        for (int i =1; i <= n1+n2; i++) {
	
            Text OutputRecord=new Text();
            OutputRecord.set(key+" "+Integer.toString(i-1)+" "+(P[i].x)+" "+(P[i].y)+" "+(P[i].z)+" "+(P[i].prev) + " "+(P[i].next));
            context.write(N,OutputRecord);
            
      
    } 
     
        
        context.write(N,EndText);     
          
  }

  }
  
  
  
//////////////////////////////////////////////////////////////
  public static void main(String[] args) throws Exception {
     
      
      JobConf conf1 = new JobConf();
      conf1.setInt("n.lines.records.token", 1);
      conf1.setInt(" NOGroups", 8);
     
      JobConf conf2 = new JobConf();
      conf2.setInt("n.lines.records.token", 1);
      conf2.setInt(" NOGroups", 8);
      
      
      
   //String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
   //if (otherArgs.length != 2) {
      //System.err.println("Usage: wordcount <in> <out>");
      //System.exit(2);
    //}
      
    //JobControl jobControl = new JobControl("jobChain");  
      
   ////////////////////////////////////////////////
      
      
   ////////////////////////////////////////////////   
      
      
    Job job1 = new Job(conf1, "GroupSort");
    job1.setJarByClass(MRMinimalist.class);
    job1.setMapperClass(GroupSortMapper.class);
    
    job1.setInputFormatClass(NLinesInputFormat.class);
    //job.setInputFormatClass(NonSplittableTextInputFormat.class);
    //Uncomment this to 
    //job.setCombinerClass(IntSumReducer.class);
    job1.setReducerClass(GroupSortReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job1, new Path("/Points/10M/"));
    FileOutputFormat.setOutputPath(job1, new Path("/SortedGPoints"));
    
    
    //System.exit(job1.waitForCompletion(true) ? 0 : 1);
    job1.waitForCompletion(true);
    
    
    //create a ControlledJob with job1 
    //ControlledJob controlledJob1 = new ControlledJob(conf1);
    //controlledJob1.setJob(job1);
    //add the job to the job control
    //jobControl.addJob(controlledJob1);
    
    ////2nd Job///
    //since we want to combine all files into one, //make sure we are only running 1 reducer
    //job2.setNumReduceTasks(1);
    
    
    Job job2 = new Job(conf2, "MainPartitionsCH");
    job2.setJarByClass(MRMinimalist.class);
    job2.setMapperClass(MainPartitions.class);
    
    job2.setInputFormatClass(NLinesInputFormat.class);
    job2.setInputFormatClass(NonSplittableTextInputFormat.class);
    //Uncomment this to 
    //job.setCombinerClass(IntSumReducer.class);
    job2.setReducerClass(MainConvexHulls.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path("/SortedGPoints"));
    FileOutputFormat.setOutputPath(job2, new Path("/MainPartitionsCH"));
    //System.exit(job2.waitForCompletion(true) ? 0 : 1);
    job2.waitForCompletion(true);
    
    
    int NoOfMerges=8;
    int Depth=0;
     String InputPath="/MainPartitionsCH";
     String OutputPath="/MergePartitionsCH";
    
    while (NoOfMerges>1){
    
     
      
      JobConf conf3 = new JobConf();
      conf3.setInt("n.lines.records.token", 1);
      conf3.setInt(" NOGroups", 8);
      conf3.setInt(" Depth", Depth);
      
      JobConf conf4 = new JobConf();
      conf4.setInt("n.lines.records.token", 1);
      conf4.setInt(" NOGroups", 8);
      conf4.setInt(" Depth", Depth);
        
      
    Job job3 = new Job(conf3, "MergePartitionsCH");
    job3.setJarByClass(MRMinimalist.class);
    job3.setMapperClass(MergePartitionsMapper.class);
    
    job3.setInputFormatClass(NLinesInputFormat.class);
    job3.setInputFormatClass(NonSplittableTextInputFormat.class);
    //Uncomment this to 
    //job.setCombinerClass(IntSumReducer.class);
    job3.setReducerClass(MergePartitionsReducer.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job3, new Path(InputPath));
    FileOutputFormat.setOutputPath(job3, new Path(OutputPath));
    //System.exit(job3.waitForCompletion(true) ? 0 : 1);
    
    //job3.waitForCompletion(true);
     
   // InputPath=OutputPath;
   // OutputPath=OutputPath+NoOfMerges;
    
    Job job4 = new Job(conf4, "MergeHulls");
    job4.setJarByClass(MRMinimalist.class);
    job4.setMapperClass(MergeHullsMapper.class);
    
    job4.setInputFormatClass(NLinesInputFormat.class);
    job4.setInputFormatClass(NonSplittableTextInputFormat.class);
    //Uncomment this to 
    //job.setCombinerClass(IntSumReducer.class);
    job4.setReducerClass(MergeHullsReducer.class);
    job4.setOutputKeyClass(Text.class);
    job4.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job4, new Path(InputPath));
    FileOutputFormat.setOutputPath(job4, new Path(OutputPath));    
        
    job4.waitForCompletion(true);    
        
        
    Depth++;
    NoOfMerges/=2;
    InputPath=OutputPath;
    OutputPath=OutputPath+NoOfMerges;
    
    }
    
    
    
    
    
    
    
  //  System.exit(job4.waitForCompletion(true) ? 0 : 1);
    
    
      
  System.exit(0); 
    
    
    
    
    //System.exit(job2.waitForCompletion(true) ? 0 : 1);
    
    
    
  }
  
  
}