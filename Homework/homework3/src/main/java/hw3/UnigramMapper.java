package hw3;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UnigramMapper extends Mapper<Object, Text, Text, Text> {

    Text mapKey;
    Text mapValue;

    public UnigramMapper(){
        mapKey = new Text();
        mapValue = new Text();
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
        while (itr.hasMoreTokens()){
            String curWord = itr.nextToken();

            mapKey.set(curWord);
            context.write(mapKey, mapValue);

            System.out.println(
                "mapKey: " + mapKey.toString() + ", " + 
                "mapValue: " + mapValue.toString()
            );
        }
    }

}
