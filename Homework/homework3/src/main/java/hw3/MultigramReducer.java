package hw3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MultigramReducer extends Reducer<Text, Text, Text, Text> {
    
    Text reduceValue;
    
    public MultigramReducer(){
        reduceValue = new Text();
    }

    public void reduce(Text key, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException 
    {
        System.out.println("key: " + key.toString());
        Map<String, Integer> docIdMap = new HashMap<>();
        for(Text docId : values){
            docIdMap.put(
                docId.toString(), 
                docIdMap.getOrDefault(docId.toString(), 0) + 1
            );
            System.out.println(
              "docId: " + docId.toString() + ", " + 
              "count: " + docIdMap.getOrDefault(docId.toString(), 0)
            );
        }

        StringBuilder sb = new StringBuilder();
        for(String docId : docIdMap.keySet()){
            sb.append(docId)
                .append(":")
                .append(docIdMap.get(docId))
                .append(" ");
        }

        if(sb.length() == 0){
          return;
        }

        reduceValue.set(sb.toString());
        context.write(key, reduceValue);

        System.out.println(
            "reduceKey: " + key.toString() + ", " +
            "reduceValue: " + reduceValue.toString()
        );
    }

}
