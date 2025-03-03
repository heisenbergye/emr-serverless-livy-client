package com.example.emr;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class EmrServerlessLivyClient implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(EmrServerlessLivyClient.class);
    private static final String SERVICE_NAME = "emr-serverless";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    private final String endpoint;
    private final String roleArn;
    private final int timeout;
    private final Aws4Signer signer;
    private final CloseableHttpClient httpClient;
    private final AwsCredentialsProvider credentialsProvider;
    private final Region region;
    
    private String sessionUrl;

    /**
     * Initialize EMR Serverless client
     *
     * @param applicationId EMR Serverless application ID
     * @param regionStr AWS region
     * @param roleArn Execution role ARN
     * @param timeout Operation timeout in seconds
     */
    public EmrServerlessLivyClient(String applicationId, String region, String roleArn, int timeout) {
        this.endpoint = "https://" + applicationId + ".livy.emr-serverless-services." + region + ".amazonaws.com";
        this.roleArn = roleArn;
        this.timeout = timeout;
        this.region = Region.of(region);
        
        // Initialize AWS credentials provider and signer
        this.credentialsProvider = DefaultCredentialsProvider.create();
        this.signer = Aws4Signer.create();
        
        // Configure HTTP client with timeouts
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(30000)
                .setSocketTimeout(timeout * 1000)
                .build();
                
        this.httpClient = HttpClients.custom()
                .setDefaultRequestConfig(requestConfig)
                .build();
                
        logger.info("Initialized EMR Serverless client with endpoint: {}", endpoint);
    }
    
    /**
     * Initialize EMR Serverless client with default timeout
     */
    public EmrServerlessLivyClient(String applicationId, String region, String roleArn) {
        this(applicationId, region, roleArn, 300);
    }
    
    /**
     * Send a signed request to EMR Serverless
     */
    private JsonNode sendRequest(HttpRequestBase request, String body, int maxRetries) throws IOException {
        // Add content-type header
        request.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        
        // If it's a request with body, set the entity
        if (request instanceof HttpEntityEnclosingRequestBase && body != null) {
            ((HttpEntityEnclosingRequestBase) request).setEntity(new StringEntity(body));
        }
        
        // Determine HTTP method
        SdkHttpMethod httpMethod;
        if (request instanceof HttpGet) {
            httpMethod = SdkHttpMethod.GET;
        } else if (request instanceof HttpPost) {
            httpMethod = SdkHttpMethod.POST;
        } else if (request instanceof HttpDelete) {
            httpMethod = SdkHttpMethod.DELETE;
        } else {
            throw new IllegalArgumentException("Unsupported HTTP method");
        }
        
        // Build SDK HTTP request for signing
        URI uri = URI.create(request.getURI().toString());
        SdkHttpFullRequest.Builder requestBuilder = SdkHttpFullRequest.builder()
                .method(httpMethod)
                .uri(uri)
                .putHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        
        // Add body content if present
        if (body != null) {
            requestBuilder.contentStreamProvider(() -> SdkBytes.fromUtf8String(body).asInputStream());
        }
        
        SdkHttpFullRequest sdkRequest = requestBuilder.build();
        
        // Create signer params
        Aws4SignerParams signerParams = Aws4SignerParams.builder()
                .signingName(SERVICE_NAME)
                .signingRegion(region)
                .awsCredentials(credentialsProvider.resolveCredentials())
                .build();
        
        // Sign the request
        SdkHttpFullRequest signedRequest = signer.sign(sdkRequest, signerParams);
        
        // Copy the signed headers to the HTTP request
        signedRequest.headers().forEach((key, values) -> {
            if (!values.isEmpty()) {
                request.setHeader(key, values.get(0));
            }
        });
        
        int retryCount = 0;
        while (retryCount <= maxRetries) {
            try {
                logger.info("Sending {} request: {}", request.getMethod(), request.getURI());
                if (body != null) {
                    logger.debug("Request body: {}", body);
                }
                
                HttpResponse response = httpClient.execute(request);
                int statusCode = response.getStatusLine().getStatusCode();
                String responseBody = EntityUtils.toString(response.getEntity());
                
                if (statusCode >= 200 && statusCode < 300) {
                    logger.info("Request successful, status code: {}", statusCode);
                    
                    // Save session URL from Location header if present
                    if (response.getFirstHeader("Location") != null) {
                        String location = response.getFirstHeader("Location").getValue();
                        logger.info("Location header: {}", location);
                        
                        // If this is a session creation response, save the session URL
                        if (request.getURI().toString().endsWith("/sessions") && request instanceof HttpPost) {
                            sessionUrl = location;
                        }
                    }
                    
                    // Return empty object if response body is empty
                    if (responseBody == null || responseBody.isEmpty()) {
                        return objectMapper.createObjectNode();
                    }
                    
                    return objectMapper.readTree(responseBody);
                } else {
                    logger.error("Request failed, status code: {}, response: {}", statusCode, responseBody);
                    
                    // Retry for certain status codes
                    if (statusCode == 429 || (statusCode >= 500 && statusCode < 600)) {
                        retryCount++;
                        if (retryCount <= maxRetries) {
                            int waitTime = (int) Math.pow(2, retryCount);
                            logger.info("Waiting {} seconds before retry...", waitTime);
                            Thread.sleep(waitTime * 1000);
                            continue;
                        }
                    }
                    
                    throw new IOException("Request failed with status code: " + statusCode + ", response: " + responseBody);
                }
            } catch (IOException e) {
                retryCount++;
                if (retryCount <= maxRetries) {
                    int waitTime = (int) Math.pow(2, retryCount);
                    logger.info("Request exception: {}. Waiting {} seconds before retry...", e.getMessage(), waitTime);
                    try {
                        Thread.sleep(waitTime * 1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Request interrupted during retry", ie);
                    }
                } else {
                    logger.error("Maximum retry count ({}) reached, giving up", maxRetries);
                    throw e;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Request interrupted during retry", e);
            }
        }
        
        throw new IOException("Failed to execute request after " + maxRetries + " retries");
    }
    
    /**
     * Create a new Spark session
     */
    public JsonNode createSession() throws IOException {
        logger.info("Creating new Spark session...");
        
        ObjectNode data = objectMapper.createObjectNode();
        data.put("kind", "pyspark");
        data.put("heartbeatTimeoutInSecond", 60);
        
        ObjectNode conf = objectMapper.createObjectNode();
        conf.put("emr-serverless.session.executionRoleArn", roleArn);
        data.set("conf", conf);
        
        String requestBody = objectMapper.writeValueAsString(data);
        HttpPost request = new HttpPost(endpoint + "/sessions");
        
        JsonNode response = sendRequest(request, requestBody, 3);
        logger.info("Session created successfully, location: {}", sessionUrl);
        return response;
    }
    
    /**
     * Wait for session to be ready
     */
    public boolean waitForSessionReady(int pollInterval) throws IOException {
        if (sessionUrl == null) {
            throw new IllegalStateException("No active session");
        }
        
        logger.info("Waiting for session to be ready...");
        long startTime = System.currentTimeMillis();
        
        while (System.currentTimeMillis() - startTime < timeout * 1000) {
            try {
                HttpGet request = new HttpGet(endpoint + sessionUrl);
                JsonNode response = sendRequest(request, null, 3);
                
                String sessionState = response.get("state").asText("unknown");
                logger.info("Current session state: {}", sessionState);
                
                if ("idle".equals(sessionState)) {
                    logger.info("Session is ready");
                    return true;
                } else if ("error".equals(sessionState) || "dead".equals(sessionState) || "killed".equals(sessionState)) {
                    logger.error("Session entered error state: {}", sessionState);
                    return false;
                }
                
                logger.info("Session not ready yet, waiting {} seconds...", pollInterval);
                Thread.sleep(pollInterval * 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Interrupted while waiting for session to be ready");
                return false;
            }
        }
        
        logger.error("Timeout ({} seconds) waiting for session to be ready", timeout);
        return false;
    }
    
    /**
     * List all sessions
     */
    public JsonNode listSessions() throws IOException {
        logger.info("Getting session list...");
        HttpGet request = new HttpGet(endpoint + "/sessions");
        return sendRequest(request, null, 3);
    }
    
    /**
     * Submit code statement to session
     */
    public Map<String, Object> submitStatement(String code) throws IOException {
        if (sessionUrl == null) {
            throw new IllegalStateException("No active session");
        }
        
        logger.info("Submitting code statement: {}", code);
        
        ObjectNode data = objectMapper.createObjectNode();
        data.put("code", code);
        
        String requestBody = objectMapper.writeValueAsString(data);
        String statementsUrl = sessionUrl + "/statements";
        HttpPost request = new HttpPost(endpoint + statementsUrl);
        
        JsonNode response = sendRequest(request, requestBody, 3);
        
        // Get statement location from response headers
        String statementLocation = null;
        // 从sendRequest方法的返回中获取statementUrl
        if (sessionUrl != null) {
            statementLocation = sessionUrl + "/statements/" + response.get("id").asText();
        }
        
        if (statementLocation == null) {
            throw new IOException("No statement location in response");
        }
        
        logger.info("Statement submitted successfully, location: {}", statementLocation);
        
        Map<String, Object> result = new HashMap<>();
        result.put("location", statementLocation);
        result.put("response", response);
        
        return result;
    }
    
    /**
     * Get statement execution result, wait until completion
     */
    public JsonNode getStatementResult(String statementLocation, int pollInterval) throws IOException {
        logger.info("Waiting for statement execution to complete...");
        long startTime = System.currentTimeMillis();
        
        while (System.currentTimeMillis() - startTime < timeout * 1000) {
            try {
                HttpGet request = new HttpGet(endpoint + statementLocation);
                JsonNode response = sendRequest(request, null, 3);
                
                String statementState = response.get("state").asText("unknown");
                logger.info("Current statement state: {}", statementState);
                
                if ("available".equals(statementState)) {
                    logger.info("Statement execution completed");
                    return response;
                } else if ("error".equals(statementState) || "cancelled".equals(statementState)) {
                    logger.error("Statement execution failed: {}", statementState);
                    return response;
                }
                
                logger.info("Statement executing, waiting {} seconds...", pollInterval);
                Thread.sleep(pollInterval * 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Interrupted while waiting for statement execution");
                return null;
            }
        }
        
        logger.error("Timeout ({} seconds) waiting for statement execution", timeout);
        return null;
    }
    
    /**
     * Delete current session
     */
    public boolean deleteSession() throws IOException {
        if (sessionUrl == null) {
            logger.warn("No active session to delete");
            return false;
        }
        
        logger.info("Deleting session...");
        HttpDelete request = new HttpDelete(endpoint + sessionUrl);
        
        try {
            sendRequest(request, null, 3);
            logger.info("Session deleted successfully");
            sessionUrl = null;
            return true;
        } catch (IOException e) {
            logger.error("Failed to delete session: {}", e.getMessage());
            return false;
        }
    }
    
    @Override
    public void close() throws Exception {
        try {
            if (sessionUrl != null) {
                deleteSession();
            }
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }
    
    /**
     * Example usage
     */
    public static void main(String[] args) {
        // Replace with your actual values
        String applicationId = "00fqi5n5j9ctg90l";
        String region = "us-west-2";
        String roleArn = "arn:aws:iam::808577411626:role/emrServerlessLivyTestRole";
        
        try (EmrServerlessLivyClient client = new EmrServerlessLivyClient(applicationId, region, roleArn)) {
            // Create session
            JsonNode sessionInfo = client.createSession();
            System.out.println("\n=== Created Session Info ===");
            System.out.println(sessionInfo.toPrettyString());
            
            // Wait for session to be ready
            if (!client.waitForSessionReady(5)) {
                System.out.println("Session failed to become ready, exiting");
                return;
            }
            
            // List sessions
            JsonNode sessions = client.listSessions();
            System.out.println("\n=== Session List ===");
            System.out.println(sessions.toPrettyString());
            
            // Submit simple calculation
            Map<String, Object> statementResult = client.submitStatement("1 + 1");
            String statementLocation = (String) statementResult.get("location");
            JsonNode statementInfo = (JsonNode) statementResult.get("response");
            
            System.out.println("\n=== Submitted Statement Info ===");
            System.out.println(statementInfo.toPrettyString());
            
            // Get result
            JsonNode result = client.getStatementResult(statementLocation, 2);
            System.out.println("\n=== Statement Execution Result ===");
            System.out.println(result.toPrettyString());
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}