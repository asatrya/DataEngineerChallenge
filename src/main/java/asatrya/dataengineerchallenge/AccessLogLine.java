package asatrya.dataengineerchallenge;

import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class represents an AWS Load Balancer Log line.
 * See https://docs.aws.amazon.com/elasticloadbalancing/latest/classic/access-log-collection.html#access-log-entry-format for more details.
 *
 * Example AWS Load Balancer log line:
 * 2015-05-13T23:39:43.945958Z my-loadbalancer 192.168.131.39:2817 10.0.0.1:80 0.000073 0.001048 0.000057 200 200 0 29 "GET http://www.example.com:80/ HTTP/1.1" "curl/7.38.0" - -
 */
public class AccessLogLine implements Serializable {

    private static final Logger logger = Logger.getLogger("Access");
    private static final String LOG_ENTRY_PATTERN = "([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) \"([^ ]*) ([^ ]*) (- |[^ ]*)\" \"(.*)\" (- |[^ ]*) (- |[^ ]*)";
    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

    private String timestamp;
    private String elb;
    private String client_ip;
    private String client_port;
    private String backend;
    private String request_processing_time;
    private String backend_processing_time;
    private String response_processing_time;
    private String elb_status_code;
    private String backend_status_code;
    private String received_bytes;
    private String sent_bytes;
    private String request_method;
    private String request_url;
    private String request_protocol;
    private String user_agent;
    private String ssl_cipher;
    private String ssl_protocol;

    public AccessLogLine(String timestamp, String elb, String client_ip, String client_port, String backend, String request_processing_time, String backend_processing_time, String response_processing_time, String elb_status_code, String backend_status_code, String received_bytes, String sent_bytes, String request_method, String request_url, String request_protocol, String user_agent, String ssl_cipher, String ssl_protocol) {
        this.timestamp = timestamp;
        this.elb = elb;
        this.client_ip = client_ip;
        this.client_port = client_port;
        this.backend = backend;
        this.request_processing_time = request_processing_time;
        this.backend_processing_time = backend_processing_time;
        this.response_processing_time = response_processing_time;
        this.elb_status_code = elb_status_code;
        this.backend_status_code = backend_status_code;
        this.received_bytes = received_bytes;
        this.sent_bytes = sent_bytes;
        this.request_method = request_method;
        this.request_url = request_url;
        this.request_protocol = request_protocol;
        this.user_agent = user_agent;
        this.ssl_cipher = ssl_cipher;
        this.ssl_protocol = ssl_protocol;
    }

    public static Logger getLogger() {
        return logger;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getElb() {
        return elb;
    }

    public void setElb(String elb) {
        this.elb = elb;
    }

    public String getClient_ip() {
        return client_ip;
    }

    public void setClient_ip(String client_ip) {
        this.client_ip = client_ip;
    }

    public String getClient_port() {
        return client_port;
    }

    public void setClient_port(String client_port) {
        this.client_port = client_port;
    }

    public String getBackend() {
        return backend;
    }

    public void setBackend(String backend) {
        this.backend = backend;
    }

    public String getRequest_processing_time() {
        return request_processing_time;
    }

    public void setRequest_processing_time(String request_processing_time) {
        this.request_processing_time = request_processing_time;
    }

    public String getBackend_processing_time() {
        return backend_processing_time;
    }

    public void setBackend_processing_time(String backend_processing_time) {
        this.backend_processing_time = backend_processing_time;
    }

    public String getResponse_processing_time() {
        return response_processing_time;
    }

    public void setResponse_processing_time(String response_processing_time) {
        this.response_processing_time = response_processing_time;
    }

    public String getElb_status_code() {
        return elb_status_code;
    }

    public void setElb_status_code(String elb_status_code) {
        this.elb_status_code = elb_status_code;
    }

    public String getBackend_status_code() {
        return backend_status_code;
    }

    public void setBackend_status_code(String backend_status_code) {
        this.backend_status_code = backend_status_code;
    }

    public String getReceived_bytes() {
        return received_bytes;
    }

    public void setReceived_bytes(String received_bytes) {
        this.received_bytes = received_bytes;
    }

    public String getSent_bytes() {
        return sent_bytes;
    }

    public void setSent_bytes(String sent_bytes) {
        this.sent_bytes = sent_bytes;
    }

    public String getRequest_method() {
        return request_method;
    }

    public void setRequest_method(String request_method) {
        this.request_method = request_method;
    }

    public String getRequest_url() {
        return request_url;
    }

    public void setRequest_url(String request_url) {
        this.request_url = request_url;
    }

    public String getRequest_protocol() {
        return request_protocol;
    }

    public void setRequest_protocol(String request_protocol) {
        this.request_protocol = request_protocol;
    }

    public String getUser_agent() {
        return user_agent;
    }

    public void setUser_agent(String user_agent) {
        this.user_agent = user_agent;
    }

    public String getSsl_cipher() {
        return ssl_cipher;
    }

    public void setSsl_cipher(String ssl_cipher) {
        this.ssl_cipher = ssl_cipher;
    }

    public String getSsl_protocol() {
        return ssl_protocol;
    }

    public void setSsl_protocol(String ssl_protocol) {
        this.ssl_protocol = ssl_protocol;
    }

    public static AccessLogLine parseFromLogLine(String logLine) {
        Matcher m = PATTERN.matcher(logLine);
        if (!m.find()) {
            logger.log(Level.WARNING, "Cannot parse logLine" + logLine);
            throw new RuntimeException("Error parsing logLine");
        }

        return new AccessLogLine(m.group(1), // timestamp
                m.group(2), // elb
                m.group(3), // client_ip
                m.group(4), // client_port
                m.group(5), // backend
                m.group(6), // request_processing_time
                m.group(7), // backend_processing_time
                m.group(8), // response_processing_time
                m.group(9), // elb_status_code
                m.group(10), // backend_status_code
                m.group(11), // received_bytes
                m.group(12), // sent_bytes
                m.group(13), // request_method
                m.group(14), // request_url
                m.group(15), // request_protocol
                m.group(16), // user_agent
                m.group(17), // ssl_cipher
                m.group(18)); // ssl_protocol
    }

    @Override
    public String toString() {
        return String.format("%s %s %s:%s %s %s %s %s %s %s %s %s \"%s %s %s\" \"%s\" %s %s",
                timestamp,
                elb, client_ip, client_port, backend, request_processing_time, backend_processing_time,
                response_processing_time, elb_status_code, backend_status_code, received_bytes,
                sent_bytes, request_method, request_url, request_protocol, user_agent, ssl_cipher, ssl_protocol);
    }
}
