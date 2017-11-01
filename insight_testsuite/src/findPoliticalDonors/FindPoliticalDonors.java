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
import java.util.regex.Pattern;

public class FindPoliticalDonors {
	
	private ObjectOutputStream output;
	private ObjectInputStream input;
	
	//InputStream inputFile = getContextClassLoader().getResourceAsStream("input");
	//InputStream inputFile1 = getContextClassLoader().getResourceAsStream("input/hello.txt");
	//InputStream outputFile1 = getContextClassLoader().getResourceAsStream("output/medianvals_by_zip.txt");
	//InputStream outputFile2 = getContextClassLoader().getResourceAsStream("input/medianvals_by_date.txt");
	
	InputStream inputStream= FindPoliticalDonors.class.getResourceAsStream("/input/itcont.txt");
	File fileName = new File("classpath:"
			+ "insight_testsuite/tests/test_1/input/itcont.txt");
	
	File input2 = new File("classpath:"
			+ "insight_testsuite/tests/test_1/output/hello.txt");
	File outputFile = new File("classpath:insight_testsuite/"
			+ "/tests/test_1/output/medianvals_by_zip.txt");
	File outputFile2 = new File("classpath:insight_testsuite/"
			+ "/tests/test_1/output/medianvals_by_date.txt");
	
	@SuppressWarnings("resource")
	public ArrayList<String> readFile() {
		ArrayList<String> dataList = new ArrayList<String>();
		try { 
	         SequenceInputStream inst=new SequenceInputStream(inputStream, new FileInputStream(input2));    
		   int j;    
		   String s = "";
		   while((j=inst.read())!=-1) {
			   if((char)j=='\n') {
				   dataList.add(s);
				   s="";
			   }
			   s = s+(char)j;
		   } 
		   medianByZipData(dataList);
		   medianByDateData(dataList);
		   inst.close();    
		  
	      } catch (EOFException eofex) {
	         System.out.println("No more records to read"+eofex);
	      } catch (IOException e ) {
	         System.out.println("Unable to close file"+e);
	      }
		return dataList; 
	}
	
	
	
	private void medianByZipData(ArrayList<String> data) {
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
		System.out.println(zip);
		
		DataOutputStream dataOut;
		try {
			dataOut = new DataOutputStream(new FileOutputStream(outputFile));
			Iterator it = zip.entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        dataOut.writeChars(pair.getValue().toString());
		        it.remove(); 
		    }
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	private void medianByDateData(ArrayList<String> data) {
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
			dataOut = new DataOutputStream(new FileOutputStream(outputFile2));
			Iterator it = date.entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        dataOut.writeChars(pair.getValue().toString());
		        it.remove(); // avoids a ConcurrentModificationException
		    }
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		}
	public static void main(String []args) {
		ReadFile r = new ReadFile();
		ArrayList<String> data = r.readFile();
		r.medianByZipData(data);
		r.medianByDateData(data);
	}
	
}
