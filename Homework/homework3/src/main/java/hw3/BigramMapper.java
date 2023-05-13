package hw3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BigramMapper extends Mapper<Object, Text, Text, Text> {

    Text mapKey;
    Text mapValue;
    Map<String, String> bigramMap;

    public BigramMapper(){
        mapKey = new Text();
        mapValue = new Text();
        bigramMap = new HashMap<>(){{
            put("science", "computer");
            put("retrieval", "information");
            put("politics", "power");
            put("angeles", "los");
            put("willis", "bruce");
        }};
    }

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException
    {
        String article = value.toString();
        if(article.isEmpty()){
            return;
        }

        int firstTabIndex = -1;
        for(int a = 0; a < article.length(); a++){
            if('\t' == article.charAt(a)){
                firstTabIndex = a;
                break;
            }
        }

        System.out.println("firstTabIndex: " + firstTabIndex);
        if(firstTabIndex < 0){
            return;
        }

        String docId = article.substring(0, firstTabIndex);
        article = article
                    .substring(firstTabIndex + 1)
                    .replaceAll("[^a-zA-Z ]", "")
                    .toLowerCase();

        mapValue.set(docId);
        StringTokenizer itr = new StringTokenizer(article);
        StringBuilder preWord = null;

        while (itr.hasMoreTokens()){
            if(preWord == null){
                preWord = new StringBuilder(itr.nextToken());
                continue;
            }

            String curWord = itr.nextToken();
            if(bigramMap.containsKey(curWord)){
                if(bigramMap.get(curWord).equals(preWord.toString())){
                    mapKey.set(preWord.toString() + " " + curWord);
                    context.write(mapKey, mapValue);

                    System.out.println(
                        "mapKey: " + mapKey.toString() + ", " + 
                        "mapValue: " + mapValue.toString()
                    );
                }
            }

            preWord.setLength(0);
            preWord.append(curWord);
        }
    }

}
