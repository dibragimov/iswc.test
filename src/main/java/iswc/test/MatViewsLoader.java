package iswc.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MatViewsLoader {
	public static List<MatView> LoadMatViews(){
		List<MatView> matViews = new ArrayList<MatView>();
		String fileName = "C:\\Users\\Dilshod\\workspace\\iswc.test\\datafiles\\matviewqueries.txt";
		try {
			BufferedReader br = new BufferedReader(new FileReader(fileName));
			try {
		        StringBuilder sb = new StringBuilder();
		        String line = br.readLine();
		        MatView currMV = null;
		        while (line != null) {
		        	if(line.startsWith("####")){
		        		if(sb.length() > 1 && currMV != null){
		        			currMV.setQuery(sb.toString());
		        			matViews.add(currMV);
		        			currMV = null;
		        		}
		        		String[] names = line.split(";");//// name;number;cost;graph
		        		int number = 0, cost = 0;
		        		number = Integer.parseInt(names[1]);
		        		cost = Integer.parseInt(names[2]);
		        		currMV= new MatView(names[0], number, names[3], cost);
		        		sb = new StringBuilder();
		        	}
		            sb.append(line);
		            sb.append(System.lineSeparator());
		            line = br.readLine();
		        }
		        if(sb.length() > 1 && currMV != null){
        			currMV.setQuery(sb.toString());
        			matViews.add(currMV);
        			currMV = null;
        		}
		    } finally {
		        br.close();
		    }
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    return matViews;
	}

	public static List<QB4OQuery> LoadQueriess(){
		List<QB4OQuery> qries = new ArrayList<QB4OQuery>();
		String fileName = "C:\\Users\\Dilshod\\workspace\\iswc.test\\datafiles\\qb4o_queries.txt";
		try {
			BufferedReader br = new BufferedReader(new FileReader(fileName));
			try {
		        StringBuilder sb = new StringBuilder();
		        String line = br.readLine();
		        QB4OQuery currMV = null;
		        while (line != null) {
		        	if(line.startsWith("####")){
		        		if(sb.length() > 1 && currMV != null){
		        			currMV.setQuery(sb.toString());
		        			qries.add(currMV);
		        			currMV = null;
		        		}
		        		String[] names = line.split(";");//// name;number;cost;graph
		        		int number = 0;
		        		number = Integer.parseInt(names[1]);
		        		currMV= new QB4OQuery(names[0], number);
		        		sb = new StringBuilder();
		        	}
		            sb.append(line);
		            sb.append(System.lineSeparator());
		            line = br.readLine();
		        }
		        if(sb.length() > 1 && currMV != null){
        			currMV.setQuery(sb.toString());
        			qries.add(currMV);
        			currMV = null;
        		}
		    } finally {
		        br.close();
		    }
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    return qries;
	}

	
}
