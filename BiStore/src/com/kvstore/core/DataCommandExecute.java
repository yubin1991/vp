package com.kvstore.core;

import java.util.List;

public class DataCommandExecute {

	private Coordinator coordinator;

	public DataCommandExecute(Coordinator coordinator) {
		this.coordinator = coordinator;
	}

	public boolean execute(String cc, String[] commandAndParams) {
		// TODO Auto-generated method stub
		if (cc.equals(Command.PUT)
				&& commandAndParams.length == Helper.NUM_OF_KEYWORDS_IN_SET_COMMAND) {
			coordinator.put(commandAndParams[1], commandAndParams[2]);

		} else if (cc.equals(Command.GET)
				&& commandAndParams.length == Helper.NUM_OF_KEYWORDS_IN_GET_COMMAND) {
			String value = (String) coordinator.get(commandAndParams[1]);
			if (value != null)
				System.out.println(value);
			else
				System.out.println("No Record");

		} else if (cc.equals(Command.GETRANGE)&& commandAndParams.length == Helper.NUM_OF_KEYWORDS_IN_GETRANGE_COMMAND) {
			   double time1=System.currentTimeMillis();
			   List l = coordinator.getRange(commandAndParams[1],commandAndParams[2]);
			   double time2=System.currentTimeMillis();
			   double time=time2-time1;
			for (int j = 0; j < l.size(); j++) {
				System.out.print(l.get(j) + "\t");
			}
			System.out.println("用时:"+time);
		} else if (cc.equals(Command.END)
				&& commandAndParams.length == Helper.NUM_OF_KEYWORDS_IN_END_COMMAND) {

		} else {
			System.out.println("Error command");
		}
		return false;
	}

}
