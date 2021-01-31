package maper;
import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimpleMaper extends Mapper<Object, Text, Text, MapWritable> {
   public void map(Object key, Text value, Context context) throws IOException, InterruptedException
   {
	   String valueStringT = value.toString();
	   String valueStringT2 = valueStringT.replace("Last update: ", "");
	   String valueString = valueStringT2.replace("Position: ", "");
	   String[] playerData = valueString.split(",");
	   MapWritable map = new MapWritable();
	   
	   boolean exists = true;
		 
	   
	   try {
		   Text name = new Text (playerData[0].trim());
			 
		   Text league = new Text(playerData[2].trim());
		   
		   String team = playerData[1].trim();
		   String age = playerData[3].trim();
		   String position = playerData[4].trim();
		   String games = playerData[5].trim();
		   String currVal = playerData[6].trim();
		   String currUpdate1 = playerData[7].trim();
		   String currUpdate2 = playerData[8].trim();
		   String highestVal = playerData[9].trim();
		   String highestUpdate = playerData[10].trim();
		   
		   String[] data = {team, age, position, games, currVal, (currUpdate1 + " " + currUpdate2), highestVal, highestUpdate};
		   
		   for(int i = 0; i < data.length; i++) {
			   if(data[i].isEmpty()) {
				   exists = false;
			   }
		   }
		   
		   Text filter = new Text("Bundesliga");
		   
		   if(exists && league.equals(filter)) {
			   map.put(name, new TextArrayWritable(data));
			   context.write(league, map);  
		   }
	   }
	   
	   catch(Exception e) {
		   
	   }
	   
		 
   }
}
