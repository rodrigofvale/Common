package com.gigasynapse.common;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.io.FileUtils;
import org.json.JSONObject;

public class ServiceConfig {
	private static ServiceConfig globalsInstance = new ServiceConfig();

	public JSONObject obj;
	
	public void load(File file) {
		String txt;
		try {
			txt = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
			obj = new JSONObject(txt);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	public static ServiceConfig getInstance() {
        return globalsInstance;
    }	
}
