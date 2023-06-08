package com.gigasynapse.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
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

	public static URLFetchStatus fetch(CloseableHttpClient httpclient, 
			String url, String content, int timeout) {
		LOGGER.fine(String.format("Fetching %s %s", url, content));
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
				LOGGER.fine(String.format("Received 204 from %s", url));
				return new URLFetchStatus(httpResponse.getStatusLine()
						.getStatusCode(), null);									
			}

			InputStream is = httpEntity.getContent();
			byte[] b = is.readAllBytes();
			EntityUtils.consumeQuietly(httpEntity);
			String answer = new String(b, StandardCharsets.UTF_8);
			LOGGER.fine(String.format("Received from %s: code %d, payload %s", 
					url, httpResponse.getStatusLine().getStatusCode(), answer));
			return new URLFetchStatus(httpResponse.getStatusLine()
					.getStatusCode(), answer);				
		} catch (IOException e) {
			if (e.getMessage().equals("Read timed out")) {
				return new URLFetchStatus(408, "");
			} else if (e.getMessage().indexOf("Connection refused") >= 0) {
				return new URLFetchStatus(500, "");
			}
			LOGGER.severe(String.format("Received %s from request\n", 
					e.getMessage()));
		}
		return new URLFetchStatus(408, "");
	}
			
	public static URLFetchStatus fetchReusingConnection(String url, 
			String content, int timeout) {
		return fetch(httpclient, url, content, timeout);
	}
	
	public static URLFetchStatus fetch(String url, String content, 
			int timeout) {
		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			return fetch(httpclient, url, content, timeout);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return new URLFetchStatus(408, "");
	}
/*
	public static URLFetchStatus doRequest(String url, String content, 
			int timeout) {
		return fetch(url, content, 1, timeout);
	}

	public static URLFetchStatus doRequest(String url, String content) {
		return fetch(url, content, 1, 15000);
	}
*/

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
