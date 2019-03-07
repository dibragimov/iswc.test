package iswc.test;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Logger {
	public static void logExecution(QB4OQuery query, MatView view, String rewrittenQuery, double milliseconds, int resultSize){
		String fileName = "C:\\Users\\Dilshod\\workspace\\iswc.test\\datafiles\\output.txt";
		try {
			BufferedWriter br = new BufferedWriter(new FileWriter(fileName, true));
			String logstr = "";
			logstr = String.format("############\nQuery name: %s; number: %s; view name: %s; view number: %s; ExecutionTime: %s; result size: %s; rewritten query: %s\n",
					query.getName(),query.getNumber(),view.getName(),view.getNumber(),milliseconds,resultSize,rewrittenQuery);
//			else if(query != null && view == null)
//				logstr = String.format("############\nQuery name: %s, number %s, text: %s; \nMatView: none\nExecutionTime: %s, result size: %s",
//						query.getName(),query.getNumber(),query.getQuery(),milliseconds,resultSize);
			br.write(logstr);
			br.flush();
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void logExecution(QB4OQuery query, double milliseconds, int resultSize){
		String fileName = "C:\\Users\\Dilshod\\workspace\\iswc.test\\datafiles\\output.txt";
		try {
			BufferedWriter br = new BufferedWriter(new FileWriter(fileName, true));
			String logstr = "";
			logstr = String.format("############\nNormal Query name: %s; number %s; ExecutionTime: %s; result size: %s\n",
					query.getName(),query.getNumber(),milliseconds,resultSize);
			br.write(logstr);
			br.flush();
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void log(String message){
		String fileName = "C:\\Users\\Dilshod\\workspace\\iswc.test\\datafiles\\messages.txt";
		try {
			BufferedWriter br = new BufferedWriter(new FileWriter(fileName, true));
			String logstr = "";
			String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
			logstr = String.format("############\nDateTime: %s; message %s\n",
					timeStamp,message);
			br.write(logstr);
			br.flush();
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
