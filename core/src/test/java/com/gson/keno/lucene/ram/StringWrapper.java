package com.gson.keno.lucene.ram;

public class StringWrapper {
    private String source;

    public String getSource() {
        return source;
    }

    public StringWrapper(String source) {
        this.source = new String(source);
    }

    public void setSource(String source) {
        this.source = source;
    }
}
