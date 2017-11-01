package findPoliticalDonors;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.SequenceInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Pattern;

public class FindPoliticalDonors {
	
	@SuppressWarnings("resource")
	public ArrayList<String> readFile(String inputFile, String outputFolder) {
		
		File inputStream = new File(inputFile);
		
		ArrayList<String> dataList = new ArrayList<String>();
		try { 
	         DataInputStream inst=new DataInputStream(new FileInputStream(inputFile));    
		   int j;    
		   String s = "";
		   while((j=inst.read())!=-1) {
			   if((char)j=='\n') {
				   dataList.add(s);
				   s="";
			   }
			   s = s+(char)j;
		   } 
		   medianByZipData(dataList, outputFolder);
		   inst.close();    
		  
	      } catch (EOFException eofex) {
	         System.out.println("No more records to read"+eofex);
	      } catch (IOException e ) {
	    	  e.printStackTrace();
	         System.out.println("Unable to close file"+e.getMessage());
	      }
		return dataList; 
	}
	
	
	
	private void medianByZipData(ArrayList<String> data, String outputFolder) {
		LinkedHashMap<String,String> zip = new LinkedHashMap<String,String>();
		//int h = 0;
		for(int i = 0; i<data.size();i++) {
			String line = data.get(i);
			String[] values = line.split(Pattern.quote("|"));
			
			String zipCode = values[10].substring(0, 5);
			if((values[15].isEmpty())) {
				if(zip.containsKey(zipCode)) {
					String tmpCons = zip.get(zipCode);
					String[] getMedians = tmpCons.split(Pattern.quote("|"));
					int count = Integer.parseInt(getMedians[3]);
					count = count+1;
					int totalAmt = (Integer.parseInt(getMedians[4]))+(Integer.parseInt(values[14]));
					int newMedian = totalAmt/count;
					String str = values[0]+"|"+zipCode+"|"+Integer.toString(newMedian)+"|"
										+Integer.toString(count)+"|"+Integer.toString(totalAmt);
					zip.put(""+i, str);
				} 
				else {
					String consStr = values[0]+"|"+zipCode+"|"+values[14]+"|1"+"|"+values[14];
					zip.put(zipCode, consStr);
				}
			}
		}
		
		DataOutputStream dataOut;
		try {
			File outputZipFile = new File(outputFolder+"/medianvals_by_zip.txt");
			if(!outputZipFile.exists())
				outputZipFile.createNewFile();
			dataOut = new DataOutputStream(new FileOutputStream(outputFolder+"/medianvals_by_zip.txt"));
			Iterator it = zip.entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        dataOut.writeChars(pair.getValue().toString());
		        it.remove(); 
		    }
			dataOut.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	private void medianByDateData(ArrayList<String> data, String outputFolder) {
		HashMap<String,String> date = new HashMap<String,String>();
		
		for(int i = 0; i<data.size();i++) {
			String line = data.get(i);
			String[] values = line.split(Pattern.quote("|"));
			if((values[15].isEmpty())) {
				if(date.containsKey(values[13])) {
					String tmpCons = date.get(values[13]);
					String[] getMedians = tmpCons.split(Pattern.quote("|"));
					int count = Integer.parseInt(getMedians[3]);
					count = count+1;
					int totalAmt = (Integer.parseInt(getMedians[4]))+(Integer.parseInt(values[14]));
					int newMedian = totalAmt/count;
					String str = values[0]+"|"+values[13]+"|"+Integer.toString(newMedian)+"|"
											+Integer.toString(count)+"|"+Integer.toString(totalAmt);
					date.put(values[13], str);
				} 
				else {
					String consStr = values[0]+"|"+values[13]+"|"+values[14]+"|1"+"|"+values[14];
					date.put(values[13], consStr);
				}
			}
		}
		DataOutputStream dataOut;
		try {
			File outputDateFile = new File(outputFolder+"/medianvals_by_date.txt");
			if(!outputDateFile.exists())
				outputDateFile.createNewFile();
			dataOut = new DataOutputStream(new FileOutputStream(outputFolder+"/medianvals_by_date.txt"));
			Iterator it = date.entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        dataOut.writeChars(pair.getValue().toString());
		        it.remove(); // avoids a ConcurrentModificationException
		    }
			dataOut.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void main(String []args) {
		
		Scanner in = new Scanner(System.in);
		String inputFile = in.next();
		String outputFolder = in.next();
		
		FindPoliticalDonors r = new FindPoliticalDonors();
		ArrayList<String> data = r.readFile(inputFile, outputFolder);
		r.medianByDateData(data, outputFolder);
	}
	
}
