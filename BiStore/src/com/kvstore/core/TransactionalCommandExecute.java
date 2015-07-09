package com.kvstore.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Stack;

public class TransactionalCommandExecute {
    private static Stack<String> transactionStack = new Stack<String>();      //track commands inside a transaction
    
  	private static Stack<DataRecord> dictionaryRecordsStack = new Stack<DataRecord>(); //track data record changes inside a transaction
		
    private static int transactionCount;                                     //track embedded transactions

	private static Coordinator coordinator;

	public TransactionalCommandExecute(Coordinator coordinator) {
		this.coordinator = coordinator;
		transactionCount = 0;
	}
	
	public static boolean inTransaction() {
		return transactionCount > 0;
	}

	/*
	 * DEAL THE COMMAND OF TRANSACTION
	 */
	public boolean execute(String cc, String command) {
		if(inTransaction() || cc.equals(Command.BEGIN)) {      //begin the transaction
			transactionStack.push(command);
		}
		//deal the rollback
		String[] commandAndParams=command.split(" ");
		if (cc.equals(Command.BEGIN) && commandAndParams.length == Helper.NUM_OF_KEYWORDS_IN_BEGIN_COMMAND) {
			transactionCount++;
		}else if(cc.equals(Command.ROLLBACK) && commandAndParams.length == Helper.NUM_OF_KEYWORDS_IN_ROLLBACK_COMMAND){
			if (transactionCount == 0) {
				printNoTransAction();
			}
			if(inTransaction()) {
				transactionCount--;
				//reverse transactions in transaction stack until you hit a BEGIN
				String lastCommand = "";
				do {
					lastCommand = transactionStack.pop();
					reverseCommand(lastCommand);
				} while(!lastCommand.equals("BEGIN"));
			} else {
				return false;
			}	
		}else if(cc.equals(Command.COMMIT) && commandAndParams.length == Helper.NUM_OF_KEYWORDS_IN_COMMIT_COMMAND) {
			if (transactionCount == 0) {
				printNoTransAction();
			}
			transactionCount = 0;
		}else if (cc.equals(Command.GET) &&  commandAndParams.length == Helper.NUM_OF_KEYWORDS_IN_GET_COMMAND) {
			System.out.println(coordinator.get(commandAndParams[1]));
		}else if (cc.equals(Command.PUT) && commandAndParams.length == Helper.NUM_OF_KEYWORDS_IN_SET_COMMAND) {
			backupValue(commandAndParams[1]);   //back the old value 
			coordinator.put(commandAndParams[1], commandAndParams[2]);
		}
		return true;
	}

	/*
	 * reverse a command
	 */
	private static void reverseCommand(String dbCommand) {
		String[] commandWords = dbCommand.split(" ");
		String firstWord = commandWords[0];
			
		if(firstWord.equals(Command.PUT)) {
		  DataRecord lastRecord = dictionaryRecordsStack.pop();
		  revertDataRecord(lastRecord);
		} else {
				//no reverse necessary
		} 
	}
	
	/*
	 * reverses a data record value change
	 */
	private static void revertDataRecord(DataRecord record) {
		coordinator.put(record.getName(), record.getValue());
	}
	
	/*
	 * back up value for a given name
	 */
	private static void backupValue(String name) {
	   if(inTransaction()) {
			//backup the present value
		   dictionaryRecordsStack.push(new DataRecord(name,coordinator.get(name)));
	   }
	}
	
	private void printNoTransAction() {
		System.out.println("NO_TRANSACTION");
	}
}
