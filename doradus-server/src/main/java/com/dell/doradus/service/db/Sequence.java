package com.dell.doradus.service.db;


public interface Sequence<T> {
    // returns null if the sequence is ended
    public T next();
}
