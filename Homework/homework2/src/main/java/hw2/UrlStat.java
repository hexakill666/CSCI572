package hw2;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.ToString;

@AllArgsConstructor
@ToString
public class UrlStat {
    int urlId;
    boolean isFetch;
    int statusCode;
    String statusDescription;
    int fileSize;
    String contentType;
    String curUrl;
    List<String> outUrlList;
}
