package com.dell.doradus.persistence.entity;

import java.util.Date;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.dell.doradus.persistence.annotation.Application;
import com.dell.doradus.persistence.annotation.Link;

@Entity
@Application(name="TestApplication", ddlAutoCreate=true, storageService="SpiderService")
@Table(name="Person")
public class Person {
	
	@Id
	@Column(name="_ID")  
	private String id;
	
	private String name;
	private int age;
	
	@Column(name="Addresses")  	
	@Link(name="Addresses", inverseName="Person", tableName="Address", fieldName="addressIds")
	private Set<String> addressIds;
	
	private Date dob;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getAge() {
		return age;
	}
	public Person setAge(int age) {
		this.age = age;
		return this;
	} 

	public Set<String> getAddressIds() {
		return addressIds;
	}	
	public void setAddressIds(Set<String> addressIds) {
		this.addressIds = addressIds;
	}
	
	public Date getDob() {
		return dob;
	}
	public void setDob(Date dob) {
		this.dob = dob;
	}
	
	@Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + age;
        result = prime * result + ((getId() == null) ? 0 : getId().hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Person other = (Person) obj;
        if (age != other.age)
            return false;
        if (getId() == null) {
            if (other.getId() != null)
                return false;
        } else if (!getId().equals(other.getId()))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        return true;
    }


}
