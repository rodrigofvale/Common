package com.gigasynapse.common;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

import org.json.JSONObject;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;

public class Http {
	private final static Logger LOGGER = Logger.getLogger("GLOBAL");
	
	public static void sendResponse(HttpExchange httpExchange, int status, JSONObject response) {
		Http.sendResponse(httpExchange, "application/json", status, response.toString());		
	}
	
	public static void sendResponse(HttpExchange httpExchange, int status, String response) {
		Http.sendResponse(httpExchange, "text/plain", status, response.toString());
	}
	
	public static void sendResponse(HttpExchange httpExchange, String contentType, int status, String response) {
		Headers headers = httpExchange.getResponseHeaders();
		headers.set("Content-Type", String.format("%s ; charset=%s", contentType, StandardCharsets.UTF_8));
		try {
			httpExchange.sendResponseHeaders(status, response.getBytes(StandardCharsets.UTF_8).length);
			OutputStream os = httpExchange.getResponseBody();
			os.write(response.getBytes(StandardCharsets.UTF_8));
			os.close();
		} catch (IOException e) {
			LOGGER.severe(String.format("Failed to send back the HTTP response %s due to  %s", response, e.getMessage()));
		}
	}
	
	public static String inputStream(InputStream is) {
		String requestString = "";
		try {
			byte[] b = is.readAllBytes();
			requestString = new String(b, StandardCharsets.UTF_8);
/*
			StringBuilder requestBuffer = new StringBuilder();
			requestString = new String(b, StandardCharsets.UTF_8);
			int rByte;
			while ((rByte = is.read()) != -1) {
				requestBuffer.append((char) rByte);
			}
			is.close();

			if (requestBuffer.length() > 0) {
				requestString = requestBuffer.toString();
			}
*/			
		} catch (IOException e) {
			e.printStackTrace();
		}
		return requestString;
	}

}
