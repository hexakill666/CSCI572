package hw3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UnigramReducer extends Reducer<Text, Text, Text, Text> {
    
    Text reduceValue;
    Map<String, Integer> map;

    public UnigramReducer(){
        reduceValue = new Text();
        map = new HashMap<>();
    }

    public void reduce(Text key, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException 
    {
        for(Text docId : values){
            map.put(
                key.toString(), 
                map.getOrDefault(docId.toString(), 0) + 1
            );
        }

        StringBuilder sb = new StringBuilder();
        for(String docId : map.keySet()){
            sb.append(docId)
                .append(":")
                .append(map.get(docId))
                .append(" ");
        }

        reduceValue.set(sb.toString());
        context.write(key, reduceValue);

        System.out.println(
            "reduceKey: " + key.toString() + 
            "reduceValue: " + reduceValue.toString()
        );
    }

}
