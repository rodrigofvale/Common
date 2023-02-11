package com.gigasynapse.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;

public class URL {
	private final static Logger LOGGER = Logger.getLogger("GLOBAL");
	private static PoolingHttpClientConnectionManager poolingConnManager = 
			new PoolingHttpClientConnectionManager();
	private static CloseableHttpClient httpclient = HttpClients.custom()
			.setConnectionManager(poolingConnManager)
			.setMaxConnPerRoute(50)
			.setMaxConnTotal(100)
			.build();

	public static String read(InputStream is) {
		String requestString = "";
		try {
			byte[] b = is.readAllBytes();
			requestString = new String(b, StandardCharsets.UTF_8);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return requestString;
	}


	public static void sendResponse(HttpExchange httpExchange, 
			String contentType, int status, String response) {
		Headers headers = httpExchange.getResponseHeaders();
		headers.set("Content-Type", String.format("%s ; charset=%s", 
				contentType, StandardCharsets.UTF_8));
		try {
			if (status == 204) {
				httpExchange.sendResponseHeaders(status, -1);				
			} else {
				httpExchange.sendResponseHeaders(status, response
						.getBytes(StandardCharsets.UTF_8).length);
				OutputStream os = httpExchange.getResponseBody();
				os.write(response.getBytes(StandardCharsets.UTF_8));
				os.close();
			}
		} catch (IOException e) {
			LOGGER.severe(String.format("Failed to send back the HTTP "
					+ "response %s due to  %s", response, e.getMessage()));
		}
	}	

	public static URLFetchStatus fetch(String url, String content, 
			int timeout, int attempts) {
		
		System.out.printf("Fetching %s %s\n", url, content);
		
		HttpPost httppost = new HttpPost(url);

		RequestConfig requestConfig = RequestConfig.custom()
				.setConnectionRequestTimeout(timeout)
				.setConnectTimeout(timeout)
				.setSocketTimeout(timeout)
				.build();

		httppost.setConfig(requestConfig);
		StringEntity params = new StringEntity(content, StandardCharsets.UTF_8);
		httppost.addHeader("content-type", "application/json; charset=UFT-8");
		httppost.setEntity(params);
		
		try (CloseableHttpResponse httpResponse = httpclient.execute(httppost)) {
			HttpEntity httpEntity = httpResponse.getEntity();
			if (httpResponse.getStatusLine().getStatusCode() == 204) {
				EntityUtils.consumeQuietly(httpEntity);
				//httppost.releaseConnection();
				return new URLFetchStatus(httpResponse.getStatusLine()
						.getStatusCode(), null);									
			}
			
			InputStream is = httpEntity.getContent();
			byte[] b = is.readAllBytes();
			EntityUtils.consumeQuietly(httpEntity);
			//httppost.releaseConnection();
			String answer = new String(b, StandardCharsets.UTF_8);
			return new URLFetchStatus(httpResponse.getStatusLine()
					.getStatusCode(), answer);				
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new URLFetchStatus(408, "");		
	}
	
/*	
	public static URLFetchStatus fetch(String url, String content, 
			int timeout, int attempts) {
		LOGGER.info(String.format("Fetching %s using payload %s", url, content));

			HttpPost httppost = new HttpPost(url);

			RequestConfig requestConfig = RequestConfig.custom()
					.setConnectionRequestTimeout(timeout)
					.setConnectTimeout(timeout)
					.setSocketTimeout(timeout)
					.build();

			httppost.setConfig(requestConfig);

			// max 10 attempts
			attempts = (attempts > 10) ? 10 : attempts; 

			StringEntity params = new StringEntity(content, StandardCharsets.UTF_8);
			httppost.addHeader("content-type", "application/json; charset=UFT-8");
			httppost.setEntity(params);
			Exception lastException = null;
			do {
				try {
					try (CloseableHttpResponse httpResponse = httpclient.execute(httppost)) {
						HttpEntity httpEntity = httpResponse.getEntity();
						if (httpResponse.getStatusLine().getStatusCode() == 204) {
							EntityUtils.consumeQuietly(httpEntity);
							//httppost.releaseConnection();
							return new URLFetchStatus(httpResponse.getStatusLine()
									.getStatusCode(), null);									
						}
						
						InputStream is = httpEntity.getContent();
						byte[] b = is.readAllBytes();
						EntityUtils.consumeQuietly(httpEntity);
						//httppost.releaseConnection();
						String answer = new String(b, StandardCharsets.UTF_8);
						return new URLFetchStatus(httpResponse.getStatusLine()
								.getStatusCode(), answer);				
					}
				} catch (Exception e) {
					attempts--;
					long mili = (long) Math.pow(10 - attempts, 2) * 1000;
					lastException = e;
					e.printStackTrace();
				}
			} while (attempts > 0);
			LOGGER.severe(String.format("Erro fetching %s using payload %s "
					+ "getting %s for timeout of %d", url, content, lastException
					.getMessage(), timeout));
		return new URLFetchStatus(408, "");
	}
*/
	public static URLFetchStatus doRequest(String url, String content, 
			int timeout) {
		return fetch(url, content, 1, timeout);
	}

	public static URLFetchStatus doRequest(String url, String content) {
		return fetch(url, content, 1, 15000);
	}


	public static String normalizeUrl(String url) {
		String normalized = "";
		String validchar = "1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-._~:/?#[]@!$&'()*+,;%=";
		for(int i = 0; i < url.length(); i++) {			
			if (validchar.indexOf(url.charAt(i)) >= 0) {
				normalized += url.charAt(i);
			} else {
				try {
					normalized += URLEncoder.encode(url.charAt(i) + "", StandardCharsets.UTF_8.toString());
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		return normalized;
	}
}
