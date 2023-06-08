package com.gigasynapse.bin;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;

import com.gigasynapse.common.ServiceConfig;
import com.gigasynapse.common.Utils;
import com.gigasynapse.db.enums.ArticleStage;
import com.gigasynapse.db.tables.ArticlesTable;
import com.gigasynapse.db.tuples.ArticleTuple;

public class CopyFromProdToDev {
	public static void main(String[] args) throws Exception {
		Class.forName("com.mysql.cj.jdbc.Driver");

		Options options = new Options();

		Option cfg = new Option("cfg", "config", true, "Define the configuration file");
		cfg.setRequired(true);
		options.addOption(cfg);
		
		Option weekOpt = new Option("w", "week", true, 
				"Index one specific week [yyyy-mm-dd]");
		weekOpt.setRequired(true);
		options.addOption(weekOpt);		

		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;//not a good practice, it serves it purpose 

		try {
			cmd = parser.parse(options, args);
		} catch (org.apache.commons.cli.ParseException e) {
			System.out.println(e.getMessage());
			formatter.printHelp("Migrate", options);
			System.exit(1);
		}
		File file = new File(cmd.getOptionValue("config"));
		ServiceConfig.getInstance().load(file);

		
			ArrayList<ArticleTuple> list = ArticlesTable.list();
			list.forEach(item -> {
				String folderIn = "/mnt/prod/k8s/data/crawler/v1.4/";
				String folderOut = "/mnt/local/crawler/v1.4/";

				String idDocAsString = String.format("%010d", item.docId);
				String[] chunks = idDocAsString.split("(?<=\\G.{3})");

				folderIn = String.format("%s/%s/%s/",
							folderIn,
							chunks[0],
							chunks[1]
							);

				folderOut = String.format("%s/%s/%s/",
							folderOut,
							chunks[0],
							chunks[1]
							);

				File folder = new File(folderOut);
				folder.mkdirs();

				String fileNameIn = String.format("%s/%d.json", folderIn, item.docId);
				String fileNameOut = String.format("%s/%d.json", folderOut, item.docId);

				File fileCheck = new File(fileNameOut);
				if (!fileCheck.exists()) {
					try {
						System.out.printf("%s -> %s\n",fileNameIn, fileNameOut);
						FileUtils.copyFile(new File(fileNameIn), new File(fileNameOut));

						fileNameIn = String.format("%s/%d.txt.compressed", folderIn, item.docId);
						fileNameOut = String.format("%s/%d.txt.compressed", folderOut, item.docId);
						System.out.printf("%s -> %s\n",fileNameIn, fileNameOut);
						FileUtils.copyFile(new File(fileNameIn), new File(fileNameOut));
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			});				
	}    
}
