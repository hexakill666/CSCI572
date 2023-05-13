package hw2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

public class MyClawler extends WebCrawler {
    
    Map<Integer, UrlStat> dataMap;
    List<String> similarUrlPrefixList;
    List<String> requireContentTypeList;
    Pattern fileExtensionPattern;
    String newsWebsiteUrl;

    public MyClawler(
        List<String> similarUrlPrefixList, 
        List<String> requireContentTypeList, 
        Pattern fileExtensionPattern, 
        String newsWebsiteUrl
    ) 
    {
        dataMap = new HashMap<>();
        this.similarUrlPrefixList = similarUrlPrefixList;
        this.requireContentTypeList = requireContentTypeList;
        this.fileExtensionPattern = fileExtensionPattern;
        this.newsWebsiteUrl = newsWebsiteUrl;
    }

    /**
     * You should implement this function to specify whether the given url
     * should be crawled or not (based on your crawling logic).
     */
    @Override
    public boolean shouldVisit(Page page, WebURL url) {
        boolean isFetch = 
            Utils.isWithinDomain(
                similarUrlPrefixList, 
                newsWebsiteUrl, 
                url.getURL().contains("?") 
                ? 
                url.getURL().substring(
                    0, 
                    url.getURL().indexOf("?")
                )
                .toLowerCase()
                : 
                url.getURL().toLowerCase()
            ) 
            &&
            !Utils.hasDiscardFileExtension(
                fileExtensionPattern, 
                url.getURL().contains("?") 
                ? 
                url.getURL().substring(
                    0, 
                    url.getURL().indexOf("?")
                )
                .toLowerCase()
                : 
                url.getURL().toLowerCase()
            );
        logger.info(
            "shouldVisit" + 
            ", urlId=" + url.getDocid() + 
            ", isFetch=" + isFetch + 
            ", contentType=" + page.getContentType() + 
            ", url=" + url.getURL()
        );
        return isFetch;
    }

    /**
     * This function is called when a page is fetched and ready to be processed
     * by your program.
     */
    @Override
    public void visit(Page page) {
        // if(
        //     !Utils.hasRequireContentType(
        //         requireContentTypeList, 
        //         page.getContentType()
        //     )
        // )
        // {
        //     dataMap.remove(page.getWebURL().getDocid());
        //     logger.info(
        //         "visit" + 
        //         ", !hasRequireContentType" + 
        //         ", urlId=" + page.getWebURL().getDocid() + 
        //         ", contentType=" + page.getContentType() + 
        //         ", url=" + page.getWebURL().getURL()
        //     );
        //     return;
        // }
        UrlStat urlStat = dataMap.getOrDefault(
            page.getWebURL().getDocid(), 
            Utils.createDefaultUrlStat(
                page.getWebURL().getDocid(), 
                page.getWebURL().getURL()
            )
        );
        urlStat.curUrl = page.getWebURL().getURL().toLowerCase();
        urlStat.isFetch = true;
        urlStat.statusCode = page.getStatusCode();
        urlStat.fileSize = page.getContentData().length;
        urlStat.contentType = 
            page.getContentType().indexOf(";") < 0 
            ? 
            page.getContentType()
            :
            page.getContentType().substring(
                0, 
                page.getContentType().indexOf(";")
            );
        if(page.getParseData() instanceof HtmlParseData){
            HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
            urlStat.outUrlList.addAll(
                htmlParseData
                .getOutgoingUrls()
                .stream()
                .map(outUrl -> outUrl.getURL().toLowerCase())
                .collect(Collectors.toList())
            );
        }
        dataMap.put(page.getWebURL().getDocid(), urlStat);
        logger.info(
            "visit, " + 
            urlStat.toString()
        );
    }

    @Override
    protected void handlePageStatusCode(WebURL webUrl, int statusCode, String statusDescription) {
        if(webUrl.getDocid() > 0){
            UrlStat urlStat = 
                dataMap.getOrDefault(
                    webUrl.getDocid(), 
                    Utils.createDefaultUrlStat(
                        webUrl.getDocid(), 
                        webUrl.getURL().toLowerCase()
                    )
                );
            urlStat.statusCode = statusCode;
            urlStat.statusDescription = statusDescription;
            dataMap.put(webUrl.getDocid(), urlStat);
        }
        logger.info(
            "handlePageStatusCode" + 
            ", urlId=" + webUrl.getDocid() + 
            ", statusCode=" + statusCode + 
            ", statusDescription=" + statusDescription + 
            ", url=" + webUrl.getURL()
        );
    }

    /**
     * This function is called by controller to get the local data of this crawler when job is
     * finished
     */
    @Override
    public Object getMyLocalData() {
        return dataMap;
    }

    // /**
    //  * This function is called by controller before finishing the job.
    //  * You can put whatever stuff you need here.
    //  */
    // @Override
    // public void onBeforeExit() {

    // }

}
