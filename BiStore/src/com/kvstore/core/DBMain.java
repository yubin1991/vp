package com.kvstore.core;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class DBMain {
	 boolean isDataCommand =true;
	 private DataCommandExecute dataCommandExecutor;
	 private TransactionalCommandExecute transactionalCommandExecutor;
	 private boolean finished;
	 
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		DBMain dbmain=new DBMain();
//		try {
//			dbmain.run();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		String str = "Finished! \n dsefwe";
		System.out.println(str.indexOf("Finished"));
		
	}
	
	public static void PutAndGet() {
		int i = 0;
		double time1 = System.currentTimeMillis();
		Coordinator<String> coordinator = new Coordinator<String>();
		while (i < 1000000) {
			String key = String.valueOf(i);
			String value =String.valueOf(i);
			coordinator.put(key, value);
			i++;
		}
		//dump the data to persistent
		coordinator.commit();
		double time2=System.currentTimeMillis();
		
		String s=coordinator.get(String.valueOf(2003));
		double time3=System.currentTimeMillis();
		
		List<String> list=coordinator.getRange(String.valueOf(2004), String.valueOf(3004));
		double time4=System.currentTimeMillis();
		
		double putTime=time2-time1;
		double getTime=time3-time2;
		double rangeTime=time4-time3;
		System.out.println(list.size()+"\n插入时间:"+putTime+"\n读取时间:"+getTime+"\n范围读取时间:"+rangeTime);
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String dbCommand = "";
		while(!dbCommand.equals(Command.END)) {
			try {
				dbCommand = br.readLine();
				String[] commandWords = dbCommand.split(" ");
				String firstWord = commandWords[0];
				if(firstWord.equals(Command.PUT) && commandWords.length == Helper.NUM_OF_KEYWORDS_IN_SET_COMMAND) {
				    coordinator.put(commandWords[1], commandWords[2]);
				} else if(firstWord.equals(Command.GET) && commandWords.length == Helper.NUM_OF_KEYWORDS_IN_GET_COMMAND) {
					String value=coordinator.get(commandWords[1]);
					if(value!=null)
						System.out.println(value);
					else 
						System.out.println("No Record");
 
				}else if(firstWord.equals(Command.GETRANGE) && commandWords.length==Helper.NUM_OF_KEYWORDS_IN_GETRANGE_COMMAND){
					List l=coordinator.getRange(commandWords[1], commandWords[2]);
					for(int j=0;j<l.size();j++){
						System.out.print(l.get(j)+"\t");
					}
				}
				else if(firstWord.equals(Command.END) && commandWords.length == Helper.NUM_OF_KEYWORDS_IN_END_COMMAND) {
					
				} else {
					System.out.println("Syntax error. Last command entered has been ignored.");
				}
				
			} catch (IOException e) {
				System.out.println("IO error while reading command.");
		        System.exit(1);
			}
		}
	}
	
	
	public void run() throws IOException {
		Coordinator<String> coordinator = new Coordinator<String>();
		
//		int i = 0;
//		double time1 = System.currentTimeMillis();
//		while (i < 2000000) {
//			String key = String.valueOf(i);
//			String value =String.valueOf(i);
//			coordinator.put(key, value);
//			i++;
//		}
//		//dump the data to persistent
//		//coordinator.commit();
//		double time2=System.currentTimeMillis();
//		
//		String s=coordinator.get(String.valueOf(2003));
//		double time3=System.currentTimeMillis();
//		
//		List<String> list=coordinator.getRange(String.valueOf(200), String.valueOf(4200));
//		double time4=System.currentTimeMillis();
//		
//		double putTime=time2-time1;
//		double getTime=time3-time2;
//		double rangeTime=time4-time3;
//		System.out.println(list.size()+"\n插入时间:"+putTime+"\n读取时间:"+getTime+"\n范围读取时间:"+rangeTime);
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String command = "";
		dataCommandExecutor=new DataCommandExecute(coordinator);
		transactionalCommandExecutor=new TransactionalCommandExecute(coordinator);
		
		while (!command.equals(Command.END)) {
			command = br.readLine();
			String[] commandAndParams = command.split(" ");
			String cc =(commandAndParams[0]);
			if (cc.equals(Command.BEGIN)) {
					isDataCommand=false;
			}
			if(isDataCommand) {
				if(!dataCommandExecutor.execute(cc,commandAndParams)) {
					//return;
				}
			}else {
				if(!transactionalCommandExecutor.execute(cc,command)) {
					//return;
				}
			}
		}
	}
	
	public void TransactionCompleted() {
		isDataCommand=true;
	}
	
}
