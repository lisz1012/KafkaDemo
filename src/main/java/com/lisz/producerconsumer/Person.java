package com.lisz.producerconsumer;

public class Person {
	private long id;

	public Person(long id, String name) {
		this.id = id;
		this.name = name;
	}

	private String name;

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	@Override
	public String toString() {
		return "Person{" +
				"id=" + id +
				", name='" + name + '\'' +
				'}';
	}

	public void setName(String name) {
		this.name = name;
	}
}
