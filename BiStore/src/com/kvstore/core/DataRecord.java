package com.kvstore.core;

/*
 * This class is used to track a name and its corresponding value in the database.
 */

public class DataRecord<T>{
	private String name;
	private T value;
	
	public DataRecord(String name, T value) {
		this.name = name;
		this.value = value;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public T getValue() {
		return value;
	}

	public void setValue(T value) {
		this.value = value;
	}
	
	
}
