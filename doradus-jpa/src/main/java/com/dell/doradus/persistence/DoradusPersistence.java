package com.dell.doradus.persistence;

import java.util.Map;

import javax.persistence.Cache;
import javax.persistence.EntityGraph;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnitUtil;
import javax.persistence.Query;
import javax.persistence.SynchronizationType;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.metamodel.Metamodel;
import javax.persistence.spi.PersistenceProvider;
import javax.persistence.spi.PersistenceUnitInfo;
import javax.persistence.spi.ProviderUtil;

public class DoradusPersistence implements PersistenceProvider {
	
    @Override
    public EntityManagerFactory createEntityManagerFactory(String persistenceUnitName, Map map) {
        return new EntityManagerFactorImpl();
    }

    @Override
    public EntityManagerFactory createContainerEntityManagerFactory(PersistenceUnitInfo info,
            Map map)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void generateSchema(PersistenceUnitInfo info, Map map) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean generateSchema(String persistenceUnitName, Map map) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ProviderUtil getProviderUtil() {
        throw new UnsupportedOperationException();
    }
    
    
    class EntityManagerFactorImpl implements EntityManagerFactory {

        @Override
        public EntityManager createEntityManager() {
            return new DoradusEntityManager();
        }

        @Override
        public EntityManager createEntityManager(Map map) {
            return new DoradusEntityManager();
        }

        @Override
        public EntityManager createEntityManager(SynchronizationType synchronizationType) {
            return new DoradusEntityManager();
        }

        @Override
        public EntityManager createEntityManager(SynchronizationType synchronizationType,
                Map map)
        {
            return new DoradusEntityManager();
        }

        @Override
        public CriteriaBuilder getCriteriaBuilder() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Metamodel getMetamodel() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public boolean isOpen() {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public void close() {
            // TODO Auto-generated method stub
            
        }

        @Override
        public Map<String, Object> getProperties() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Cache getCache() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public PersistenceUnitUtil getPersistenceUnitUtil() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void addNamedQuery(String name, Query query) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public <T> T unwrap(Class<T> cls) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public <T> void addNamedEntityGraph(String graphName, EntityGraph<T> entityGraph) {
            // TODO Auto-generated method stub
            
        }        
    }

}
