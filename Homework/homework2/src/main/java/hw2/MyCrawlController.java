package hw2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class MyCrawlController {

    static Logger logger = LoggerFactory.getLogger(MyCrawlController.class);

    String storageFolder;
    int politeDelay;
    int crawlDepth;
    int maxPageSize;
    boolean isIncludeBinary;
    boolean isResumableCrawl;

    int multiThreadSize;
    List<String> similarUrlPrefixList;
    List<String> requireContentTypeList;
    Pattern fileExtensionPattern;
    String newsWebsiteUrl;

    public CrawlConfig createCrawlConfig() {
        CrawlConfig crawlConfig = new CrawlConfig();
        crawlConfig.setCrawlStorageFolder(storageFolder);
        crawlConfig.setPolitenessDelay(politeDelay);
        crawlConfig.setMaxDepthOfCrawling(crawlDepth);
        crawlConfig.setMaxPagesToFetch(maxPageSize);
        crawlConfig.setIncludeBinaryContentInCrawling(isIncludeBinary);
        crawlConfig.setResumableCrawling(isResumableCrawl);
        return crawlConfig;
    }

    public Map<Integer, UrlStat> start() {
        logger.info("prepare crawling");
        CrawlConfig crawlConfig = createCrawlConfig();
        PageFetcher pageFetcher = new PageFetcher(crawlConfig);
        Map<Integer, UrlStat> dataMap = new HashMap<>();
        try {
            CrawlController crawlController = new CrawlController(
                crawlConfig, 
                pageFetcher, 
                new RobotstxtServer(
                    new RobotstxtConfig(), 
                    pageFetcher
                )
            );
            crawlController.addSeed(newsWebsiteUrl);
            CrawlController.WebCrawlerFactory<MyClawler> crawlFactory = 
                () -> new MyClawler(
                    similarUrlPrefixList, 
                    requireContentTypeList, 
                    fileExtensionPattern, 
                    newsWebsiteUrl
                );
            logger.info("start crawling");
            crawlController.startNonBlocking(crawlFactory, multiThreadSize);
            crawlController.waitUntilFinish();
            logger.info("finish crawling");
            List<Object> dataList = crawlController.getCrawlersLocalData();
            for(Object o : dataList){
                dataMap.putAll((HashMap<Integer, UrlStat>) o);
            }
        } 
        catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return dataMap;
    }

}
