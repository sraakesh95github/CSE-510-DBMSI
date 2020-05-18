package tests;

import bigt.BigT;
import bigt.Stream;
import btree.WriteToFile;
import global.AttrType;
import heap.Heapfile;
import heap.Tuple;
import global.RID;
import global.SystemDefs;

public class QueryInterface {
	
	String write_filepath = "C:\\SR files\\College\\Subjects\\DBMSI\\Project\\Phase 3\\Demo version\\Group 4\\Minibase\\query_output.txt";
	
	public static String[] unpadAttributes(String RL, String CL, String Value) {
		String[]ToUnPad= new String[3];
		ToUnPad[0] = RL.substring(2+Integer.parseInt(RL.substring(0,2)));
		ToUnPad[1] = CL.substring(2+Integer.parseInt(CL.substring(0,2)));
		ToUnPad[2] = Value.substring(2+Integer.parseInt(Value.substring(0,2)));
		return ToUnPad;
	}
	
	public QueryInterface (String filename, int type, int orderType, String rowFilter, String colFilter, String valueFilter) {
		
		
		

		
		Tuple t = null;
		  t = new Tuple();
	      AttrType[] attrType = new AttrType[4];
	      attrType[0] = new AttrType(AttrType.attrString);
	      attrType[1] = new AttrType(AttrType.attrString);
	      attrType[2] = new AttrType(AttrType.attrString);
	      //attrType[3] = new AttrType(AttrType.attrString);
	      attrType[3] = new AttrType(AttrType.attrInteger);
	      short[] attrSize = new short[4];
	      attrSize[0] = 30;
	      attrSize[1] = 30;
	      attrSize[2] = 50;
	      
	      
	      try {
	          t.setHdr((short) 4, attrType, attrSize);
	        }
	        catch (Exception e) {
	          e.printStackTrace();
	        }

	      //Get the size of the tuple
	      int size = t.size();
	      
	      //Create a Tuple with the pre-defined size
	      t = new Tuple(size);
	      
		
		//Init the Map structure with the headers
	      try {
	          t.setHdr((short) 4, attrType, attrSize);
	        }
	        catch (Exception e) {
	          e.printStackTrace();
	        }
	      
	    String rowLabel = null;
	    String columnLabel = null;
	    String value = null;
	    int timestamp = 0;
	    String[] outval = new String[3];
	    
	    RID rid = null;
	    rid = new RID();
	    
		Stream stream = null;
		try 
		{
			String write_string = null;
			String filename_heapfile=filename+"_heap.in";
			stream = BigT.openStream(filename_heapfile,orderType, rowFilter, colFilter, valueFilter);
			int count=1;
			t=stream.getNext(rid,filename_heapfile);
			while (t != null)
			{
				rowLabel = t.getStrFld(1);
				columnLabel = t.getStrFld(2);
				value = t.getStrFld(3);
				timestamp = t.getIntFld(4);
				outval = unpadAttributes(rowLabel, columnLabel, value);
				write_string = "Element#: " + count + "\nRow Label: " + outval[0] + "\nColumn Label: " + outval[1] + "\nValue: " + outval[2] + "\nTimestamp: " + timestamp + "\n\n";
				System.out.println("\nElement "+count+":");
				System.out.println("Row Label: "+ outval[0]);
				System.out.println("Column Label: "+ outval[1]);
				System.out.println("Value: "+ outval[2]);
				System.out.println("Time stamp: "+ timestamp);
				count++;
				
				writeFile(write_string, write_filepath);
				
				t = stream.getNext(rid,filename_heapfile);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}

		//stream.closeStream();
		
	}
	
	public void writeFile(String dataWrite, String filepath) {
		WriteToFile data = new WriteToFile(filepath, true);
		try {
		data.WriteFile(dataWrite);
		//System.out.println("BTreeName written...");
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
}
