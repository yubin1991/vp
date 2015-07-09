package com.kvstore.core;

import java.io.Serializable;
import java.util.HashMap;

import com.kvstore.core.Index;

public class Indexer implements Serializable{
   private final HashMap<String,Index> map;
   
   public Indexer(){
	   this.map=new HashMap();
   }
   
   public Index get(String key){
	   return map.get(key);
   }
   
   public void put(String key,Index value){
	   this.map.put(key, value);
   }
}
