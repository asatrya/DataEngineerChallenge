package asatrya.dataengineerchallenge;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AccessLogLineTest {

    @Test
    public void testParseFromLogLineShortAgent(){
        String logLine = "2015-05-13T23:39:43.945958Z my-loadbalancer 192.168.131.39:2817 10.0.0.1:80 0.000073 0.001048 0.000057 200 200 0 29 \"GET http://www.example.com:80/ HTTP/1.1\" \"curl/7.38.0\" - -";
        AccessLogLine accessLogLine = AccessLogLine.parseFromLogLine(logLine);
        assertEquals(logLine, accessLogLine.toString());
    }

    @Test
    public void testParseFromLogLineLongAgent(){
        String logLine = "2015-07-22T09:00:35.458709Z marketpalce-shop 37.228.105.99:58560 10.0.4.227:80 0.000025 0.003448 0.000018 200 200 0 2048 \"GET https://paytm.com:443/styles/app.css HTTP/1.1\" \"Opera/9.80 (Android; Opera Mini/7.6.40251/37.6066; U; en) Presto/2.12.423 Version/12.16\" ECDHE-RSA-AES128-SHA TLSv1";
        AccessLogLine accessLogLine = AccessLogLine.parseFromLogLine(logLine);
        assertEquals(logLine, accessLogLine.toString());
    }

    @Test
    public void testParseFromLogLineEmptyAgent(){
        String logLine = "2015-07-22T09:00:35.821161Z marketpalce-shop 54.251.151.39:44280 10.0.4.244:80 0.00002 0.003225 0.000022 200 200 0 70316 \"GET https://paytm.com:443/sellers/wp-json/posts HTTP/1.1\" \"-\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2";
        AccessLogLine accessLogLine = AccessLogLine.parseFromLogLine(logLine);
        assertEquals(logLine, accessLogLine.toString());
    }

    @Test
    public void testParseFromLogLineEmptyBackend(){
        String logLine = "2015-07-22T10:34:46.178711Z marketpalce-shop 115.112.62.211:41340 - -1 -1 -1 504 0 0 0 \"POST https://paytm.com:443/shop/log HTTP/1.1\" \"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)\" ECDHE-RSA-AES128-SHA TLSv1";
        AccessLogLine accessLogLine = AccessLogLine.parseFromLogLine(logLine);
        assertEquals(logLine, accessLogLine.toString());
    }
}
