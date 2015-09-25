package com.dell.doradus.service.db.s3;

public class ListItem {
    public String name;
    public boolean hasContents;
    public ListItem(String name, boolean hasContents) {
        this.name = name;
        this.hasContents = hasContents;
    }
}
