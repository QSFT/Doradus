package com.dell.doradus.persistence;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.sql.Date;
import java.text.ParseException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.Before;
import org.junit.Test;

import com.dell.doradus.persistence.entity.Address;
import com.dell.doradus.persistence.entity.Person;

public class DoradusPersistenceTest {
	
	EntityManager entityManager;
	
	@Before
	public void setup() {
		
    	EntityManagerFactory emf = Persistence.createEntityManagerFactory("myapp");
        
    	entityManager = emf.createEntityManager();	       
	}
	
	@Test
	public void testPersistAndRetrieveEntity() throws ParseException {	
		//persist Addresses
		Address homeAddress = new Address();
		homeAddress.setStreet("34212 Orcas");
		homeAddress.setCity("Renton");
		homeAddress.setState("WA");
		homeAddress.setZip("98665");
		entityManager.persist(homeAddress);
		assertNotNull(homeAddress.getId());		

				
		Address workAddress = new Address();
		workAddress.setStreet("111 Main St");
		workAddress.setCity("AV");
		workAddress.setState("CA");
		workAddress.setZip("92656");
		entityManager.persist(workAddress);
		assertNotNull(workAddress.getId());		

			
		//persist Person with 2 addresses
		Person person = new Person();
		person.setAge(40);
		person.setName("John");
		Set<String> addressIds= new HashSet<String>(Arrays.asList(homeAddress.getId(), workAddress.getId()));

		person.setAddressIds(addressIds);
		person.setDob(Date.valueOf("1967-11-22"));
		
		assertNull(person.getId());		
		
		//test persist
		entityManager.persist(person);
		assertNotNull(person.getId());		
		
		Person person2 = new Person();
		person2.setAge(45);
		person2.setName("Marry");
		person2.setAddressIds(new HashSet<String>(Arrays.asList(homeAddress.getId())));
				
		entityManager.persist(person2);
		assertNotNull(person2.getId());		
		
		//test retrieval
		Person result = entityManager.find(Person.class, person.getId());
		assertNotNull(result.getId());		
		assertEquals(person.getName(), result.getName());
		assertEquals(person.getAge(), result.getAge());	
		assertNotNull(person.getDob());
		
		//verify Link object result
		Set<String>addresses = person.getAddressIds();
		
		Address addressResult = entityManager.find(Address.class, addresses.iterator().next());
		assertNotNull(addressResult.getState());
	}	
	
	@Test
	public void testUpdate() {
		//persist Addresses
		Address address = new Address();
		address.setStreet("5 Polaris");
		address.setCity("Aliso Viejo");
		address.setState("CA");
		address.setZip("98665");
		entityManager.persist(address);
		assertNotNull(address.getId());		
		String newZip = "92656";
		address.setZip(newZip);
		entityManager.persist(address);
		
		Address result = entityManager.find(Address.class, address.getId());
		assertEquals(newZip, result.getZip());
	}
	
	@Test
	public void testDelete() {
		Person person = new Person();
		person.setAge(40);
		person.setName("Peter");		
		entityManager.persist(person);
		
		assertNotNull(entityManager.find(Person.class, person.getId()).getName());
		
		entityManager.remove(person);
		
		assertNull(entityManager.find(Person.class, person.getId()).getName());
	}	
}
