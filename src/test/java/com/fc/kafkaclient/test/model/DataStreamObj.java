package com.fc.kafkaclient.test.model;

public class DataStreamObj {

	private int id;
	private String message;
	
	public DataStreamObj() {}
	
	public DataStreamObj(int id, String message) {
		this.id = id;
		this.message = message;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	@Override
	public String toString() {
		return "DataStreamObj [id=" + id + ", message=" + message + "]";
	}
}
