package com.fc.kafkaclient;

public class KafkaClientException extends RuntimeException {

	private static final long serialVersionUID = -2657049157420538088L;

	public KafkaClientException(String message, String... parameters) {
		super(message + " " + parameters);
	}
	
	public KafkaClientException(String message,Throwable e) {
		super(message, e);
	}
	
	public KafkaClientException(Throwable e, String message, String... parameters) {
		super(message + " " + parameters, e);
	}
}
