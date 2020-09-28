package com.fc.kafkaclient;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KafkaClientConfig {

	public static Logger LOGGER = Logger.getLogger(KafkaClientConfig.class.getName());
	public static final String PROPERTY = "kafkaClient.properties.file";
	public static final String FILENAME = "application.properties";
	
	private KafkaClientConfig() {
		throw new IllegalStateException();
	}
	
	public static Properties loadConfig() {
		Properties properties = new Properties();
		String filePath = System.getenv(PROPERTY);
		try {
			if(filePath != null && !filePath.isEmpty()) {
				properties.load(new FileInputStream(new File(filePath)));
			} else {
				URL url = Thread.currentThread().getContextClassLoader().getResource("application.properties");
				properties.load(url.openStream());
			}
			
			return properties;
		} catch(Exception e) {
			LOGGER.log(Level.SEVERE, "Failed to read the properties file.", e);
		}
		
		throw new KafkaClientException("Failed to read the properties file.");
	}
	
}