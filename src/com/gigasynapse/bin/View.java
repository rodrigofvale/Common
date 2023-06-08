package com.gigasynapse.bin;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;

import com.gigasynapse.common.Utils;

public class View {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		byte dataCompressed[] = FileUtils.readFileToByteArray(new File(args[0]));
		byte dataDecompressed[] = Utils.decompress(dataCompressed);
		System.out.println(new String(dataDecompressed, StandardCharsets.UTF_8));
	}

}
