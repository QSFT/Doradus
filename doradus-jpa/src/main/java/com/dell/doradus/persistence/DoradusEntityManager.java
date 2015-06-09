package com.dell.doradus.persistence;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.persistence.EntityGraph;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.Query;
import javax.persistence.StoredProcedureQuery;
import javax.persistence.Table;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaDelete;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.CriteriaUpdate;
import javax.persistence.metamodel.Metamodel;

import com.datastax.driver.core.DataType;
import com.datastax.driver.mapping.EntityTypeParser;
import com.datastax.driver.mapping.meta.EntityFieldMetaData;
import com.datastax.driver.mapping.meta.EntityTypeMetadata;
import com.dell.doradus.client.ApplicationSession;
import com.dell.doradus.client.Client;
import com.dell.doradus.client.Credentials;
import com.dell.doradus.client.SpiderSession;
import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.BatchResult;
import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.DBObject;
import com.dell.doradus.common.DBObjectBatch;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.FieldType;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.persistence.annotation.Application;
import com.dell.doradus.persistence.annotation.Link;

public class DoradusEntityManager implements EntityManager {

	private static Client client;
	private static final String DB_PROP_FILE = "doradus.properties";
	
	public DoradusEntityManager() {
		loadClient();

	}
	
	private void loadClient() {
		if (client == null) {
			final Properties properties = new Properties();
			try {
				properties.load(this.getClass().getResourceAsStream("/"+DB_PROP_FILE));
				String host = properties.getProperty("doradus.host");
				String port = properties.getProperty("doradus.port");	
				String apiPrefix = properties.getProperty("doradus.apiPrefix");	
				client = new Client(host, Integer.parseInt(port), apiPrefix);
				Credentials creds = new Credentials(properties.getProperty("doradus.tenant"),properties.getProperty("doradus.user"), properties.getProperty("doradus.password"));
		        client.setCredentials(creds);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void persist(Object entity) {
		//retrieve Application from annotation
		Application applicationAnnotation = (Application) entity.getClass().getAnnotation(Application.class);	
		String applicationName = applicationAnnotation.name();
	
		boolean ddlAutoCreate = applicationAnnotation.ddlAutoCreate();
		String storageService = applicationAnnotation.storageService();
		
		Table tableAnnotation = (Table)entity.getClass().getAnnotation(Table.class);
		String tableName = tableAnnotation.name();
		Set<Link> linkFields = getLinkFields(entity.getClass());
    	Class<?> clazz = entity.getClass();
        EntityTypeMetadata entityMetadata = EntityTypeParser.getEntityMetadata(clazz);
        
        List<EntityFieldMetaData> fields = entityMetadata.getFields();
        String[] columns = new String[fields.size()];
        Object[] values = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            EntityFieldMetaData f = fields.get(i);
            columns[i] = f.getColumnName();
            values[i] = f.getValue(entity);
        }     

        //only create schema for application that never has one.
        if (client.getAppDef(applicationName) == null) {			
        	if (ddlAutoCreate) {
        		//create new application
        		ApplicationDefinition appDef = new ApplicationDefinition();
        		appDef.setAppName(applicationName);
        		createTable(storageService, tableName, linkFields, fields, appDef, clazz);
        		client.createApplication(appDef);
        	}
        }		
        
        ApplicationSession session = (ApplicationSession)client.openApplication(applicationName);
        TableDefinition tableDef = session.getAppDef().getTableDef(tableName);
        if (tableDef == null) {
     		createTable(storageService, tableName, linkFields, fields, session.getAppDef(), clazz);  
     		client.createApplication(session.getAppDef());
        }
        
	    DBObjectBatch objectBatch = new DBObjectBatch();
	    
	    DBObject dbObject = new DBObject();
	    dbObject.setTableName(tableName);
	    
	    for (int i = 0; i < fields.size(); i++) {
	    	if (columns[i] != null && values[i] != null) {
	    		if (fields.get(i).getDataType().equals(DataType.Name.SET)) {
	    			dbObject.addFieldValues(columns[i], (Set)values[i]);
	    		}
	    		else if (fields.get(i).getDataType().equals(DataType.Name.TIMESTAMP)) {
    				dbObject.addFieldValue(columns[i], values[i].toString());    			
	    		}
	    		else {
	    			dbObject.addFieldValue(columns[i], values[i].toString());
	    		}
    		}
    	}    
	
	    objectBatch.addObject(dbObject);

		
        storageService = session.getAppDef().getStorageService();
        //persist
    	BatchResult result = ((SpiderSession)session).addBatch(tableName, objectBatch);
        if (result.isFailed()) {
        	throw new RuntimeException(result.getErrorMessage());
        }
        
        EntityFieldMetaData idField = entityMetadata.getFieldMetadata("id");
        idField.setValue(entity, result.getResultObjects().iterator().next());	        
	}

	@Override
	public <T> T merge(T entity) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void remove(Object entity) {
		//retrieve Application from annotation
		Application applicationAnnotation = (Application) entity.getClass().getAnnotation(Application.class);	
		String applicationName = applicationAnnotation.name();
	
		Table tableAnnotation = (Table)entity.getClass().getAnnotation(Table.class);
		String tableName = tableAnnotation.name();
		
	   	Class<?> clazz = entity.getClass();
        EntityTypeMetadata entityMetadata = EntityTypeParser.getEntityMetadata(clazz);
        
        EntityFieldMetaData idField = entityMetadata.getFieldMetadata("id");
        idField.getValue(entity);
        
        DBObjectBatch objectBatch = new DBObjectBatch();
        DBObject dbObj1 = new DBObject();
        dbObj1.setObjectID((String)idField.getValue(entity));
        dbObj1.setTableName(tableName);
        dbObj1.setDeleted(true);
        objectBatch.addObject(dbObj1);
        ApplicationSession session = (ApplicationSession)client.openApplication(applicationName);
        BatchResult batchResult = session.deleteBatch(tableName, objectBatch);
    	
        if (batchResult.isFailed()) {
        	throw new RuntimeException(batchResult.getErrorMessage());
        }
	}

	@Override
	public <T> T find(Class<T> entityClass, Object primaryKey) {
    	Application applicationAnnotation = (Application) entityClass.getAnnotation(Application.class);	
    	//retrieve Application from annotation
		String applicationName = applicationAnnotation.name();

		Table tableAnnotation = (Table)entityClass.getAnnotation(Table.class);
		String tableName = tableAnnotation.name();
		
        ApplicationSession session = (ApplicationSession)client.openApplication(applicationName);
        String storageService = session.getAppDef().getStorageService();
        
        if (storageService.startsWith("Spider")) {
        	DBObject dbObject = ((SpiderSession)session).getObject(tableName, (String)primaryKey);
        	String table = dbObject.getTableName();
            return createEntityObjectFromDBObject(entityClass, dbObject);            
        }
        return null;	        
	}

	@Override
	public <T> T find(Class<T> entityClass, Object primaryKey,
			Map<String, Object> properties) {
		 throw new UnsupportedOperationException();
	}

	@Override
	public <T> T find(Class<T> entityClass, Object primaryKey,
			LockModeType lockMode) {
		 throw new UnsupportedOperationException();
	}

	@Override
	public <T> T find(Class<T> entityClass, Object primaryKey,
			LockModeType lockMode, Map<String, Object> properties) {
		 throw new UnsupportedOperationException();
	}

	@Override
	public <T> T getReference(Class<T> entityClass, Object primaryKey) {
		 throw new UnsupportedOperationException();
	}

	@Override
	public void flush() {
		 throw new UnsupportedOperationException();
	}

	@Override
	public void setFlushMode(FlushModeType flushMode) {
		 throw new UnsupportedOperationException();
	}

	@Override
	public FlushModeType getFlushMode() {
		 throw new UnsupportedOperationException();
	}

	@Override
	public void lock(Object entity, LockModeType lockMode) {
		 throw new UnsupportedOperationException();	
	}

	@Override
	public void lock(Object entity, LockModeType lockMode,
			Map<String, Object> properties) {
		 throw new UnsupportedOperationException();		
	}

	@Override
	public void refresh(Object entity) {
		 throw new UnsupportedOperationException();		
	}

	@Override
	public void refresh(Object entity, Map<String, Object> properties) {
		 throw new UnsupportedOperationException();		
	}

	@Override
	public void refresh(Object entity, LockModeType lockMode) {
		 throw new UnsupportedOperationException();		
	}

	@Override
	public void refresh(Object entity, LockModeType lockMode,
			Map<String, Object> properties) {
		 throw new UnsupportedOperationException();	
	}

	@Override
	public void clear() {
		 throw new UnsupportedOperationException();	
	}

	@Override
	public void detach(Object entity) {
		 throw new UnsupportedOperationException();	}

	@Override
	public boolean contains(Object entity) {
		 throw new UnsupportedOperationException();
	}

	@Override
	public LockModeType getLockMode(Object entity) {
		 throw new UnsupportedOperationException();
	}

	@Override
	public void setProperty(String propertyName, Object value) {
		 throw new UnsupportedOperationException();		
	}

	@Override
	public Map<String, Object> getProperties() {
		 throw new UnsupportedOperationException();
	}

	@Override
	public Query createQuery(String qlString) {
		 throw new UnsupportedOperationException();
	}

	@Override
	public <T> TypedQuery<T> createQuery(CriteriaQuery<T> criteriaQuery) {
		 throw new UnsupportedOperationException();
	}

	@Override
	public Query createQuery(CriteriaUpdate updateQuery) {
		 throw new UnsupportedOperationException();
	}

	@Override
	public Query createQuery(CriteriaDelete deleteQuery) {
		 throw new UnsupportedOperationException();
	}

	@Override
	public <T> TypedQuery<T> createQuery(String qlString, Class<T> resultClass) {
		 throw new UnsupportedOperationException();
	}

	@Override
	public Query createNamedQuery(String name) {
		 throw new UnsupportedOperationException();
	}

	@Override
	public <T> TypedQuery<T> createNamedQuery(String name, Class<T> resultClass) {
		 throw new UnsupportedOperationException();
	}

	@Override
	public Query createNativeQuery(String sqlString) {
		 throw new UnsupportedOperationException();
	}

	@Override
	public Query createNativeQuery(String sqlString, Class resultClass) {
		 throw new UnsupportedOperationException();
	}

	@Override
	public Query createNativeQuery(String sqlString, String resultSetMapping) {
		 throw new UnsupportedOperationException();
	}

	@Override
	public StoredProcedureQuery createNamedStoredProcedureQuery(String name) {
		 throw new UnsupportedOperationException();
	}

	@Override
	public StoredProcedureQuery createStoredProcedureQuery(String procedureName) {
		 throw new UnsupportedOperationException();
	}

	@Override
	public StoredProcedureQuery createStoredProcedureQuery(
			String procedureName, Class... resultClasses) {
		 throw new UnsupportedOperationException();
	}

	@Override
	public StoredProcedureQuery createStoredProcedureQuery(
			String procedureName, String... resultSetMappings) {
		 throw new UnsupportedOperationException();
	}

	@Override
	public void joinTransaction() {
		 throw new UnsupportedOperationException();
	}

	@Override
	public boolean isJoinedToTransaction() {
		 throw new UnsupportedOperationException();
	}

	@Override
	public <T> T unwrap(Class<T> cls) {
		 throw new UnsupportedOperationException();
	}

	@Override
	public Object getDelegate() {
		 throw new UnsupportedOperationException();
	}

	@Override
	public void close() {
		 throw new UnsupportedOperationException();
	}

	@Override
	public boolean isOpen() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public EntityTransaction getTransaction() {
		 throw new UnsupportedOperationException();
	}

	@Override
	public EntityManagerFactory getEntityManagerFactory() {
		 throw new UnsupportedOperationException();
	}

	@Override
	public CriteriaBuilder getCriteriaBuilder() {
		 throw new UnsupportedOperationException();
	}

	@Override
	public Metamodel getMetamodel() {
		 throw new UnsupportedOperationException();
	}

	@Override
	public <T> EntityGraph<T> createEntityGraph(Class<T> rootType) {
		 throw new UnsupportedOperationException();
	}

	@Override
	public EntityGraph<?> createEntityGraph(String graphName) {
		 throw new UnsupportedOperationException();
	}

	@Override
	public EntityGraph<?> getEntityGraph(String graphName) {
		 throw new UnsupportedOperationException();
	}

	@Override
	public <T> List<EntityGraph<? super T>> getEntityGraphs(Class<T> entityClass) {
		 throw new UnsupportedOperationException();
	}

	private void createTable(String storageService, String tableName,
			Set<Link> linkFields, List<EntityFieldMetaData> fields,
			ApplicationDefinition appDef, Class entityClass) {
		TableDefinition tableDef = new TableDefinition(appDef);
		tableDef.setTableName(tableName);
		tableDef.setApplication(appDef);
		appDef.addTable(tableDef);
		appDef.setOption(CommonDefs.OPT_STORAGE_SERVICE, storageService);
		for (int i = 0; i < fields.size(); i++) {    
			FieldDefinition fieldDef = new FieldDefinition(tableDef);	 
			if (FieldDefinition.isValidFieldName(fields.get(i).getColumnName())) {
    			if (!setLinkField(fields.get(i).getName(), fieldDef, entityClass)) {   				
					switch (fields.get(i).getDataType()) {
			    		case BOOLEAN:
			    			fieldDef.setName(fields.get(i).getColumnName());
			    			fieldDef.setType(FieldType.BOOLEAN);
			    			break;
			    		case INT:
			    			fieldDef.setName(fields.get(i).getColumnName());
			    			fieldDef.setType(FieldType.INTEGER);
			    			break;
			    		case TEXT:
			    			fieldDef.setName(fields.get(i).getColumnName());
			    			fieldDef.setType(FieldType.TEXT);
			    			
			    			break;
			    		case TIMESTAMP:
			    			fieldDef.setName(fields.get(i).getColumnName());
			    			fieldDef.setType(FieldType.TIMESTAMP);
			    			break;
			    		default:
			    			break;
			    	}
    			}
				tableDef.addFieldDefinition(fieldDef);		
		    }
		}
	}

	private boolean setLinkField(String name, FieldDefinition fieldDef, Class entityClass) {
		Field[] fields = entityClass.getDeclaredFields(); 
		for (Field f: fields) {
			Link linkAnnotation = f.getAnnotation(Link.class) ;
			if (linkAnnotation!= null) {
				if (name.equals(linkAnnotation.fieldName())) {
					fieldDef.setName(linkAnnotation.name());
					fieldDef.setType(FieldType.LINK);
					fieldDef.setCollection(true);
					fieldDef.setLinkInverse(linkAnnotation.inverseName());
					fieldDef.setLinkExtent(linkAnnotation.tableName());				
					return true;
				}
				
			}
		}
		return false;

	}

	private Set<Link> getLinkFields(Class entityClass) {
		Set<Link>linkFields = new HashSet<Link>();		
		Field[] fields = entityClass.getDeclaredFields(); 
		for (Field f: fields) {
			if (f.getAnnotation(Link.class) != null) {
				linkFields.add(f.getAnnotation(Link.class));
			}
		}
		return linkFields;
	}
	private <T> T createEntityObjectFromDBObject(Class<T> entityClass,
			DBObject dbObject) {
		T entity = null;
		try {
			entity = entityClass.newInstance();
		
		    EntityTypeMetadata entityMetadata = EntityTypeParser.getEntityMetadata(entityClass);
		    
		    // set properties' values
		    for (EntityFieldMetaData field : entityMetadata.getFields()) {
		    	Object value = getValueObjectFromDBObject(dbObject, field);
		    	if (value != null) {
		    		field.setValue(entity, value);
		    	}
		    }
		    return entity;    
		}  
		catch (Exception e) {
			e.printStackTrace();
		    return null;
		}
	}   
	private Object getValueObjectFromDBObject(DBObject dbObject, EntityFieldMetaData field) {
		Object value = null;
		List<String> fieldValues = null;
        DataType.Name dataType = field.getDataType();
		switch (dataType) {
  
            case BOOLEAN:
                value = Boolean.valueOf(dbObject.getFieldValue(field.getColumnName()));
                break;
            case TEXT:
                value = dbObject.getFieldValue(field.getColumnName());
                break;
            case TIMESTAMP:
				try {
					String dateField = dbObject.getFieldValue(field.getColumnName());
					if (dateField != null) {
						value = Utils.parseDate(dateField).getTime();
					}
				} catch (Exception e) {
					value = null;
				}
                break;
            case INT:
    			try {
					String intField = dbObject.getFieldValue(field.getColumnName());
					if (intField != null) {
						value = Integer.valueOf(dbObject.getFieldValue(field.getColumnName()));
					}
				} catch (Exception e) {
					value = null;
				}
                break;
            case BIGINT:
      			try {
    				String longField = dbObject.getFieldValue(field.getColumnName());
    				if (longField != null) {
    					value = Long.valueOf(dbObject.getFieldValue(field.getColumnName()));
    				}
				} catch (Exception e) {
					value = null;
				}
                break;    
            case DOUBLE:
      			try {
    				String doubleField = dbObject.getFieldValue(field.getColumnName());
    				if (doubleField != null) {
    					value = Double.valueOf(dbObject.getFieldValue(field.getColumnName()));
    				}
				} catch (Exception e) {
					value = null;
				}            	
               break;
            case FLOAT:
      			try {
    				String floatField = dbObject.getFieldValue(field.getColumnName());
    				if (floatField != null) {
    					value = Float.valueOf(dbObject.getFieldValue(field.getColumnName()));
    				}
				} catch (Exception e) {
					value = null;
				}                 	
               break;
            case LIST:
                if (value == null) {
                    value = new ArrayList<Object>();
                }
            	fieldValues = dbObject.getFieldValues(field.getColumnName());
             	if (fieldValues != null && !fieldValues.isEmpty()) {
                    ((List<Object>) value).addAll(fieldValues);
                }
                break;
            case SET:
                if (value == null) {
                    value = new HashSet<Object>();
                }
            	fieldValues = dbObject.getFieldValues(field.getColumnName());
            	if (fieldValues != null && !fieldValues.isEmpty()) {
	                ((Set<Object>) value).addAll(new HashSet<String>(fieldValues));
                }
                break;
            default:
            	dbObject.getFieldValue(field.getColumnName());
                break;
        }
		return value;
	}	

}
