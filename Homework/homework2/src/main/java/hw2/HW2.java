package hw2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.http.HttpStatus;

public class HW2 {
    public static void main(String[] args) {
        String storageFolder = "/tmp/mydata/";
        int politeDelay = 1000;
        int crawlDepth = 16;
        int maxPageSize = 20000;
        boolean isIncludeBinary = true;
        boolean isResumableCrawl = false;

        int multiThreadSize = 5;
        List<String> similarUrlPrefixList = new ArrayList<String>(){{
            add("//www.");
            add("https://");
            add("http://");
        }};
        List<String> requireContentTypeList = new ArrayList<String>(){{
            add("image/");
            add("text/html");
            add("application/pdf");
            add("application/msword");
        }};
        Pattern fileExtensionPattern = Pattern.compile(
            ".*\\.(css|js|mid|mp2|mp3|mp4|wav|avi|mov|mpeg|ram|m4v|rm|" + 
            "smil|wmv|swf|wma|zip|rar|gz|rss|woff2|webmanifest)$"
        );
        String newsWebsiteName = "latimes";
        String newsWebsiteUrl = "https://www.latimes.com";

        String dataFileName = "mydata.txt";
        String fetchFileName = "fetch_" + newsWebsiteName + ".csv";
        String visitFileName = "visit_" + newsWebsiteName + ".csv";
        String urlsFileName = "urls_" + newsWebsiteName + ".csv";

        String reportFileName = "CrawlReport_" + newsWebsiteName + ".txt";
        String name = "YourName";
        String uscId = "0123456789";
        int kb = 1024;
        int mb = 1024 * kb;

        MyCrawlController myCrawlController = 
            new MyCrawlController(
                storageFolder, 
                politeDelay, 
                crawlDepth, 
                maxPageSize, 
                isIncludeBinary, 
                isResumableCrawl, 
                multiThreadSize, 
                similarUrlPrefixList, 
                requireContentTypeList, 
                fileExtensionPattern, 
                newsWebsiteUrl
            );
        Map<Integer, UrlStat> dataMap = myCrawlController.start();

        writeMyData(dataMap, dataFileName);
        writeFetchCSV(dataMap, fetchFileName);
        writeVisitCSV(dataMap, visitFileName);
        writeCrawlReport(
            dataMap, 
            reportFileName, 
            name, 
            uscId, 
            multiThreadSize, 
            kb, 
            mb, 
            newsWebsiteUrl, 
            similarUrlPrefixList, 
            requireContentTypeList
        );
    }

    public static void writeMyData(Map<Integer, UrlStat> dataMap, String dataFileName) {
        try(
            BufferedWriter bufferedWriter = new BufferedWriter(
                new FileWriter(
                    new File(dataFileName)
                )
            );
        )
        {
            for(Integer key : dataMap.keySet()){
                Utils.writeWithNewLine(
                    bufferedWriter, 
                    dataMap.get(key).toString()
                );
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void writeFetchCSV(Map<Integer, UrlStat> dataMap, String fetchFileName) {
        try(
            BufferedWriter bufferedWriter = new BufferedWriter(
                new FileWriter(
                    new File(fetchFileName)
                )
            );
        )
        {
            Utils.writeWithNewLine(
                bufferedWriter, 
                "URL, Status"
            );
            for(Integer key : dataMap.keySet()){
                UrlStat urlStat = dataMap.get(key);
                Utils.writeWithNewLine(
                    bufferedWriter, 
                    urlStat.curUrl + ", " + urlStat.statusCode
                );
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void writeVisitCSV(Map<Integer, UrlStat> dataMap, String visitFileName) {
        try(
            BufferedWriter bufferedWriter = new BufferedWriter(
                new FileWriter(
                    new File(visitFileName)
                )
            );
        )
        {
            Utils.writeWithNewLine(
                bufferedWriter, 
                "URL, Size(Bytes), # of Outlinks, Content-Type"
            );
            for(Integer key : dataMap.keySet()){
                UrlStat urlStat = dataMap.get(key);
                if(urlStat.statusCode == HttpStatus.SC_OK){
                    Utils.writeWithNewLine(
                        bufferedWriter, 
                        urlStat.curUrl + ", " + 
                        urlStat.fileSize + ", " + 
                        urlStat.outUrlList.size() + ", " + 
                        urlStat.contentType
                    );
                }
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void writeCrawlReport(
        Map<Integer, UrlStat> dataMap, 
        String reportFileName, 
        String name, 
        String uscId, 
        int multiThreadSize, 
        int kb, 
        int mb, 
        String newsWebsiteUrl, 
        List<String> similarUrlPrefixList, 
        List<String> requireContentTypeList
    ) 
    {
        try(
            BufferedWriter bufferedWriter = new BufferedWriter(
                new FileWriter(
                    new File(reportFileName)
                )
            );
        )
        {
            int fetchSucceedSize = 0;
            int totalExtractUrlSize = 0;
            Set<String> extractUrlSet = new HashSet<>();
            int uniqueInsideExtractUrlSize = 0;
            int uniqueOutsideExtractUrlSize = 0;
            int[] fileSizeTable = new int[5];
            Map<String, Integer> statusMap = new HashMap<>();
            Map<String, Integer> contentTypeMap = new HashMap<>();

            for(Integer key : dataMap.keySet()){
                UrlStat urlStat = dataMap.get(key);
                if(urlStat.statusCode == HttpStatus.SC_OK){
                    fetchSucceedSize++;
                    fileSizeTable[Utils.getFileLevel(kb, mb, urlStat.fileSize)]++;
                    contentTypeMap.put(
                        urlStat.contentType, 
                        contentTypeMap.getOrDefault(urlStat.contentType, 0) + 1
                    );
                    totalExtractUrlSize += urlStat.outUrlList.size();
                    extractUrlSet.addAll(urlStat.outUrlList);
                }
                String statusKey = 
                    urlStat.statusCode + " " + 
                    urlStat.statusDescription;
                statusMap.put(
                    statusKey, 
                    statusMap.getOrDefault(statusKey, 0) + 1
                );
            }

            for(String extractUrl : extractUrlSet){
                if(
                    Utils.isWithinDomain(
                        similarUrlPrefixList, 
                        newsWebsiteUrl, 
                        extractUrl
                    )
                )
                {
                    uniqueInsideExtractUrlSize++;
                }
                else{
                    uniqueOutsideExtractUrlSize++;
                }
            }

            Utils.writeWithNewLine(bufferedWriter, "Name: " + name);
            Utils.writeWithNewLine(bufferedWriter, "USC ID: " + uscId);
            Utils.writeWithNewLine(bufferedWriter, "News Site crawled: " + newsWebsiteUrl);
            Utils.writeWithNewLine(bufferedWriter, "Number of threads: " + multiThreadSize);
            Utils.writeWithNewLine(bufferedWriter, "");
            Utils.writeWithNewLine(bufferedWriter, "Fetch Statistics: ");
            Utils.writeWithNewLine(bufferedWriter, "==================");
            Utils.writeWithNewLine(bufferedWriter, "# fetches attempted: " + dataMap.size());
            Utils.writeWithNewLine(bufferedWriter, "# fetches succeed: " + fetchSucceedSize);
            Utils.writeWithNewLine(bufferedWriter, "# fetches failed or aborted: " + (dataMap.size() - fetchSucceedSize));
            Utils.writeWithNewLine(bufferedWriter, "");
            Utils.writeWithNewLine(bufferedWriter, "Outgoing URLs: ");
            Utils.writeWithNewLine(bufferedWriter, "==================");
            Utils.writeWithNewLine(bufferedWriter, "Total URLs extracted: " + totalExtractUrlSize);
            Utils.writeWithNewLine(bufferedWriter, "# unique URLs extracted: " + extractUrlSet.size());
            Utils.writeWithNewLine(bufferedWriter, "# unique URLs within News Site: " + uniqueInsideExtractUrlSize);
            Utils.writeWithNewLine(bufferedWriter, "# unique URLs outside News Site: " + uniqueOutsideExtractUrlSize);
            Utils.writeWithNewLine(bufferedWriter, "");
            Utils.writeWithNewLine(bufferedWriter, "Status Codes: ");
            Utils.writeWithNewLine(bufferedWriter, "==================");
            for(String statusKey : statusMap.keySet()){
                Utils.writeWithNewLine(
                    bufferedWriter, 
                    statusKey + ": " + statusMap.get(statusKey)
                );
            }
            Utils.writeWithNewLine(bufferedWriter, "");
            Utils.writeWithNewLine(bufferedWriter, "File Sizes: ");
            Utils.writeWithNewLine(bufferedWriter, "==================");
            Utils.writeWithNewLine(bufferedWriter, "< 1KB: " + fileSizeTable[0]);
            Utils.writeWithNewLine(bufferedWriter, "1KB - <10KB: " + fileSizeTable[1]);
            Utils.writeWithNewLine(bufferedWriter, "10KB - <100KB: " + fileSizeTable[2]);
            Utils.writeWithNewLine(bufferedWriter, "100KB - <1MB: " + fileSizeTable[3]);
            Utils.writeWithNewLine(bufferedWriter, ">=1MB: " + fileSizeTable[4]);
            Utils.writeWithNewLine(bufferedWriter, "");
            Utils.writeWithNewLine(bufferedWriter, "Content Types: ");
            Utils.writeWithNewLine(bufferedWriter, "==================");
            for(String contentTypeKey : contentTypeMap.keySet()){
                if(
                    Utils.hasRequireContentType(
                        requireContentTypeList, 
                        contentTypeKey
                    )
                )
                {
                    Utils.writeWithNewLine(
                        bufferedWriter, 
                        contentTypeKey + ": " + contentTypeMap.get(contentTypeKey)
                    );
                }
            }

        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

}
