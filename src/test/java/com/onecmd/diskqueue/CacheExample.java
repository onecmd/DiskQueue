package com.onecmd.diskqueue;

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class CacheExample {

    private static AtomicInteger autoId = new AtomicInteger(0);

    private int id;
    private String name;
    private String addtionalText;
    private Date time;

    public CacheExample(){

    }

    public CacheExample(String name, String addtionalText) {
        id = autoId.incrementAndGet();
        this.name = name;
        this.addtionalText = addtionalText;
        time = new Date();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddtionalText() {
        return addtionalText;
    }

    public void setAddtionalText(String addtionalText) {
        this.addtionalText = addtionalText;
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    public boolean equals(CacheExample example){
        if(this.id != example.getId())
            return false;
        if(!this.name.equals(example.getName()))
            return false;
        if(!this.addtionalText.equals(example.getAddtionalText()))
            return false;
        if(!this.time.equals(example.getTime()))
            return false;

        return true;
    }
}
