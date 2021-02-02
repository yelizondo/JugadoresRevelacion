package reducer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import maper.TextArrayWritable;

enum Positions{
	Forward,
	Midfielder,
	Goalkeeper,
	Defender
}

public class SimpleReducer extends Reducer<Text, MapWritable, Text, Text> { 
	
    public void reduce(Text key, Iterable<MapWritable> values, Context context ) throws IOException, InterruptedException { 
    	Text player = null;
    	Writable[] data = null;
    	Date cu = null;
    	Date cu2 = null;
    	 
        SimpleDateFormat f = new SimpleDateFormat("MM/dd/yy");
        SimpleDateFormat f2 = new SimpleDateFormat("MMM d yyyy");
        
        float score = 0;
        

    	for(MapWritable val : values) {
    		player = (Text) val.keySet().toArray()[0];
    		
    		data = ((TextArrayWritable) val.get(player)).get();

    		int age = Integer.parseInt(data[1].toString());
    		String position = ((Text) data[2]).toString();
    		float currVal = Float.parseFloat(data[4].toString());
    		float highVal = Float.parseFloat(data[6].toString());
    		
    		try {
				cu2 = f.parse(data[7].toString());						//currentUpdate
				cu = f2.parse(data[5].toString().replace("\"", ""));	//highUpdate
			} catch (ParseException e) {
				e.printStackTrace();
			}
    		
    		float dayDiff = Math.abs(cu2.getTime() - cu.getTime())/ (1000 * 60 * 60 * 24);
    		float pricePercent = currVal / highVal;
    		
    		score = calculateScore(pricePercent, dayDiff, position, age);
    		
    		String t = player.toString() + ", " + score;
    		System.out.println(t);
    		
    		context.write(new Text(), new Text(t));
    	}
		
		//context.write(player, mapF);
    }
    
    
    float calculateScore(float pricePercentage, float dayDiff, String position, int age) {
    	float res = 1;
    	
    	res = pricePercentage * dayScore(dayDiff) * calcPos(position) * calcAge(age);

    	
    	return res;
    }
    
    
    float calcAge(int age) {
    	float res;
    	if(age >= 21 && age <= 26) res = 1;
    	else if ( (age >= 30 && age <=33) || (age >= 18 && age <= 19)) res = 0.9f;
    	else res = 0.8f;
    	return res;
    }
    
    
    float calcPos(String position) {
    	float res;
    	if(position.equals("Forward")) res = 1;
    	else if(position.equals("Midfielder")) res = 0.975f;
    	else if(position.equals("Goalkeeper")) res = 0.950f;
    	else res = 0.925f;
    	return res;
    }
    
    
    float dayScore(float dayDiff) {
    	float score;
    	
    	if(dayDiff <= 138) score = 1;
    	else if(dayDiff > 139 && dayDiff <= 365) score = 0.85f;
    	else if(dayDiff > 366 && dayDiff <= 548) score = 0.7f;
    	else score = 0.6f;
    	return score;
    }
     
}