package com.dell.doradus.persistence.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.dell.doradus.persistence.annotation.Application;

@Entity
@Application(name="TestApplication", ddlAutoCreate=true, storageService="SpiderService")
@Table(name="Address")
public class Address {
	@Id
	@Column(name="_ID")  
	private String id;
	
	@Column(name="Street")  	
	private String street;
	
	@Column(name="City")  		
	private String city;
	
	@Column(name="State")  		
	private String state;
	
	@Column(name="Zip")  			
	private String zip;
	
	public String getStreet() {
		return street;
	}
	public void setStreet(String street) {
		this.street = street;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	public String getZip() {
		return zip;
	}
	public void setZip(String zip) {
		this.zip = zip;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}

}
