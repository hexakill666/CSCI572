package hw1;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.stream.Collectors;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

public class HW1 {
    public static void main( String[] args ) {
        int sleepTimeMin = 10;
        int sleepTimeMax = 100;
        String queryFileName = "100QueriesSet4.txt";
        String googleResultFileName = "Google_Result4.json";
        String searchUrl = "https://www.duckduckgo.com/html/?q=";
        String hw1JsonFileName = "hw1.json";
        String hw1CSVFileName = "hw1.csv";

        List<String> queryList = readQuery(queryFileName);
        writeHW1Json(crawleQuery(queryList, searchUrl, sleepTimeMin, sleepTimeMax), hw1JsonFileName);

        Map<String, List<String>> googleResultMap = readResultToMap(googleResultFileName);
        Map<String, List<String>> searchResultMap = readResultToMap(hw1JsonFileName);
        List<QueryStat> queryStatList = calStat(queryList, googleResultMap, searchResultMap);
        writeHW1CSV(queryStatList, hw1CSVFileName);
    }

    public static List<String> readQuery(String queryFileName) {
        List<String> res = new ArrayList<>();
        try (
            Scanner sc = new Scanner(
                Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream(queryFileName)
            );
        ) 
        {
            while(sc.hasNextLine()){
                res.add(sc.nextLine().strip());
            }
        } 
        catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }

    public static int getRandomSleepTime(int sleepTimeMin, int sleepTimeMax) {
        return new Random().nextInt(sleepTimeMax + 1 - sleepTimeMin) + sleepTimeMin;
    }

    public static Map<String, List<String>> crawleQuery(List<String> queryList, String searchUrl, int sleepTimeMin, int sleepTimeMax) {
        Map<String, List<String>> res = new HashMap<>();
        try {
            for(int a = 0; a < queryList.size(); a++){
                System.out.println("start crawle query " + (a + 1) + ": " + queryList.get(a));
                Document doc = Jsoup.connect(searchUrl + queryList.get(a)).get();
                Elements divList = doc.getElementById("links").getElementsByClass("web-result");
                List<String> searchResultList = new ArrayList<>();
                for(Element div : divList){
                    String link = div.getElementsByClass("result__a").first().attr("href");
                    System.out.println("link: " + link);
                    searchResultList.add(link);
                }
                res.put(queryList.get(a), searchResultList);
                System.out.println("down crawle query " + (a + 1) + ": " + queryList.get(a));
                if(a < queryList.size() - 1){
                    try {
                        int curSleepTime = getRandomSleepTime(sleepTimeMin, sleepTimeMax) * 1000;
                        System.out.println("start sleep " + curSleepTime + "ms");
                        Thread.sleep(curSleepTime);
                        System.out.println("done sleep " + curSleepTime + "ms");
                    } 
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } 
        catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }

    public static void writeHW1Json(Map<String, List<String>> searchResultMap, String hw1JsonFileName) {
        try (
            BufferedWriter bufferedWriter = new BufferedWriter(
                new FileWriter(
                    new File(hw1JsonFileName)
                )
            );
        ) 
        {
            bufferedWriter.write(
                new GsonBuilder()
                .disableHtmlEscaping()
                .create()
                .toJson(searchResultMap)
            );
        } 
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Map<String, List<String>> readResultToMap(String resultFileName) {
        Map<String, List<String>> res = null;
        try (
            Scanner sc = new Scanner(
                "hw1.json".equals(resultFileName) 
                ? 
                new FileInputStream(resultFileName)
                :
                Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream(resultFileName)
            );
        ) 
        {
            StringBuilder sb = new StringBuilder();
            while(sc.hasNextLine()){
                sb.append(sc.nextLine());
            }
            res = new Gson().fromJson(
                sb.toString(), 
                new TypeToken<Map<String, List<String>>>(){}
            );
        } 
        catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }

    public static String getSimilarUrl(List<String> similarUrlPrefixList, String curUrl) {
        if(!curUrl.isEmpty() && curUrl.charAt(curUrl.length() - 1) == '/'){
            curUrl = curUrl.substring(0, curUrl.length() - 1);
        }
        for(String similarUrlPrefix : similarUrlPrefixList){
            int index = curUrl.indexOf(similarUrlPrefix);
            if(index >= 0){
                return curUrl.substring(index + similarUrlPrefix.length());
            }
        }
        return curUrl;
    }

    public static double calSpearmanRHO(List<int[]> resultRankList) {
        if(resultRankList.isEmpty()){
            return 0;
        }
        else if(resultRankList.size() == 1){
            int[] curRank = resultRankList.get(0);
            return curRank[0] == curRank[1] ? 1 : 0;
        }
        else{
            double diffSum = 0;
            for(int[] curRank : resultRankList){
                diffSum += (curRank[0] - curRank[1]) * (curRank[0] - curRank[1]);
            }
            return 1.0 - 6.0 * diffSum / (resultRankList.size() * (resultRankList.size() * resultRankList.size() - 1));
        }
    }

    public static List<QueryStat> calStat(List<String> queryList, Map<String, List<String>> googleResultMap, Map<String, List<String>> searchResultMap) {
        List<QueryStat> res = new ArrayList<>();
        List<String> similarUrlPrefixList = new ArrayList<>(){{
            add("www.");
            add("https://");
            add("http://");
        }};
        for(int a = 0; a < queryList.size(); a++){
            String curQuery = queryList.get(a);

            List<String> googleSimilarUrlList = 
            googleResultMap.get(curQuery).stream()
            .map(curUrl -> getSimilarUrl(similarUrlPrefixList, curUrl))
            .collect(Collectors.toList());

            List<String> searchSimilarUrlList = 
            searchResultMap.get(curQuery).stream()
            .limit(10)
            .map(curUrl -> getSimilarUrl(similarUrlPrefixList, curUrl))
            .collect(Collectors.toList());

            List<int[]> resultRankList = new ArrayList<>();
            for(int b = 0; b < googleSimilarUrlList.size(); b++){
                int index = searchSimilarUrlList.indexOf(googleSimilarUrlList.get(b));
                if(index >= 0){
                    resultRankList.add(new int[]{b + 1, index + 1});
                }
            }

            res.add(
                new QueryStat(
                    a + 1, 
                    curQuery, 
                    resultRankList.size(), 
                    resultRankList.size() * 100.0 / googleSimilarUrlList.size(), 
                    calSpearmanRHO(resultRankList)
                )
            );
        }
        return res;
    }

    public static void writeHW1CSV(List<QueryStat> queryStatList, String hw1CSVFileName) {
        try (
            BufferedWriter bufferedWriter = new BufferedWriter(
                new FileWriter(
                    new File(hw1CSVFileName)
                )
            );
        ) 
        {
            int overlapSum = 0;
            double overlapPercentSum = 0;
            double rhoSum = 0;
            System.out.println("Queries, Number of Overlapping Results, Percent Overlap, Spearman Coefficient");
            bufferedWriter.write("Queries, Number of Overlapping Results, Percent Overlap, Spearman Coefficient");
            bufferedWriter.newLine();
            for(QueryStat qs : queryStatList){
                overlapSum += qs.overlapCount;
                overlapPercentSum += qs.overlapPercent;
                rhoSum += qs.rho;
                System.out.println(qs);
                bufferedWriter.write(qs.toString());
                bufferedWriter.newLine();
            }
            String averageStr = "Averages, " + 
                (overlapSum * 1.0 / queryStatList.size()) + ", " + 
                (overlapPercentSum / queryStatList.size()) + ", " +
                (rhoSum / queryStatList.size());
            System.out.println(averageStr);
            bufferedWriter.write(averageStr);
            bufferedWriter.newLine();
        } 
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class QueryStat{
    int queryId;
    String queryName;
    int overlapCount;
    double overlapPercent;
    double rho;

    public QueryStat(int queryId, String queryName, int overlapCount, double overlapPercent, double rho){
        this.queryId = queryId;
        this.queryName = queryName;
        this.overlapCount = overlapCount;
        this.overlapPercent = overlapPercent;
        this.rho = rho;
    }

    public String toString() {
        return "Query " + queryId + ", " + overlapCount + ", " + overlapPercent + ", " + rho;
    }
}