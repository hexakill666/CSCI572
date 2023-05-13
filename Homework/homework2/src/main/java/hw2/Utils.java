package hw2;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class Utils{

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

    public static boolean isWithinDomain(List<String> similarUrlPrefixList, String newsWebsiteUrl, String curUrl) {
        return getSimilarUrl(similarUrlPrefixList, curUrl)
            .startsWith(getSimilarUrl(similarUrlPrefixList, newsWebsiteUrl));
    }

    public static boolean hasRequireContentType(List<String> requireContentTypeList, String curContentType) {
        for(String requireContentType : requireContentTypeList){
            if(curContentType.contains(requireContentType)){
                return true;
            }
        }
        return false;
    }

    public static boolean hasDiscardFileExtension(Pattern fileExtensionPattern, String curUrl) {
        return fileExtensionPattern.matcher(curUrl).matches();
    }

    public static UrlStat createDefaultUrlStat(int docId, String url) {
        return new UrlStat(
            docId, 
            false,
            0, 
            "null", 
            0, 
            "null", 
            url, 
            new ArrayList<>()
        );
    }

    public static int getFileLevel(int kb, int mb, int curFileSize) {
        if(curFileSize >= mb){
            return 4;
        }
        else if(100 * kb <= curFileSize && curFileSize < mb){
            return 3;
        }
        else if(10 * kb <= curFileSize && curFileSize < 100 * kb){
            return 2;
        }
        else if(kb <= curFileSize && curFileSize < 10 * kb){
            return 1;
        }
        else{
            return 0;
        }
    }

    public static void writeWithNewLine(BufferedWriter bufferedWriter, String content) {
        try {
            bufferedWriter.write(content);
            bufferedWriter.newLine();
        } 
        catch (IOException e) {
            e.printStackTrace();
        }
    }

}