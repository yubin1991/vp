/*
 * The is the Coordinator
 * It receive the user's request
 * Accord the user's key ,decide to the database node and then get data from it
 * 
 */

package com.kvstore.core;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Coordinator<T> {
	private static final int NODENUM = 10;  // 数据库节点数
	private BitCask<T> bitCask[];           // 操作变量
	private Indexer indexer;                // 保存全局索引
	private String FilePath="G:/mydb";
	private File directory;

	/*
	 *  init the all the store nodes
	 */
	public Coordinator() { 
		this.bitCask = new BitCask[NODENUM];
		this.directory = new File(FilePath);
		if (!this.directory.exists()) {
			this.directory.mkdir();
		}
		for (int i = 0; i < NODENUM; i++) {
			this.bitCask[i] = BitCask.of(directory.getAbsolutePath()+File.separator
					+ String.valueOf(i));                 // 以数字取名为服务器的名字
		}
	}
	
	/*
	 * put the key-value to the Database 
	 * The key is named by the integer 
	 * Get the DATABase node by the key 
	 * Params key value 
	 * Return boolean 
	 */
	public boolean put(String key, T value) {
		if(key == null || value==null){
			try {
				throw  new Exception("key or value cannot be null");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return false;
		}
		int node_num = Integer.parseInt(key) % NODENUM;
		this.bitCask[node_num].put(key, value);
		return true;
	}

	
	/*
	 * Get value from DataBase by key 
	 * Get the node by key params key 
	 * return Object t
	 */
	public T get(String key) {
		if(key==null){
			try {
				throw  new Exception("key  cannot be null");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		}
		int parseKey = Integer.parseInt(key);
		int node_num = parseKey % NODENUM;
		return bitCask[node_num].get(key);
	}
	
	/*
	 * Dump the index to disk file
	 * The index is resident memory
	 * This operation can be done for backup
	 */
	public void dumpIndexToFile(){
		for (int i = 0; i < NODENUM; i++) {
			this.bitCask[i].dumpIndexTo(directory.getAbsolutePath()+File.separator
					+ String.valueOf(i)+".index");
		}
	}
	
	/*
	 * get a range data by  key from key1 to key2
	 * all the value between the two key
	 * return a list of objects
	 */
	public List<T> getRange(String keystart,String keyend){
		if(keystart==null || keyend==null){
			try {
				throw  new Exception("key  cannot be null");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		}
		
		int start=Integer.parseInt(keystart);
		int end=Integer.parseInt(keyend);
		int min=start<end?start:end;
		int max=start<end?end:start;
		List<T> list=new ArrayList<T>();
		
		for(int i=min;i<=max;i++){
			list.add(this.get(String.valueOf(i)));
		}
		return list;
	}
	
	/*
	 * 
	 */
	public void commit(){
		for(int i=0;i<bitCask.length;i++){
			bitCask[i].commit();
		}
	}
	
}
