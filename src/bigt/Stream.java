package bigt;

import heap.Scan;
import btree.WriteToFile;
import bigt.BigT;

import java.io.*;
import java.lang.*;
import heap.Heapfile;
import btree.*;
import global.*;
import heap.Tuple;
import index.IndexScan;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;
import java.util.*;



public class Stream {
	
	String filepath = "C:\\SR files\\College\\Subjects\\DBMSI\\Project\\Phase 3\\Demo version\\Group 4\\Minibase\\src\\btree\\BtreeCheck.txt";
	
	private static int BtreeCounter = 0;

	public void writeBtreeName(String dataWrite, String filepath) {
		WriteToFile data = new WriteToFile(filepath, true);
		try {
		data.WriteFile(dataWrite);
		//System.out.println("BTreeName written...");
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	  public String timeStampToString(int ts) {
		  String stringToBeCvt = null;
		  stringToBeCvt = Integer.toString(ts);
		  int n = stringToBeCvt.length();
		  for(int i=6; i>n; i--) {
			  stringToBeCvt = '0' + stringToBeCvt;
		  }
		  return stringToBeCvt;
	  }
	
	  public String readFile (String filepath){ 
	  // We need to provide file path as the parameter: 
	  // double backquote is to avoid compiler interpret words 
	  // like \test as \t (ie. as a escape sequence) 
	  File readFile = new File(filepath); 
	  
	  BufferedReader br = null;
	  
	  try {
	  br = new BufferedReader(new FileReader(readFile)); 
	  }
	  catch(Exception e) {
		  e.printStackTrace();
	  }
	  
	  String st = null; 
	  String prevst = null;
	  try {
	  while ((st = br.readLine()) != null) {
	  //  System.out.println(st);
	    prevst = st;
	  }
	  }
	  catch(Exception e) {
		  e.printStackTrace();
	  }
	  return prevst;
	  }
	  
	
	
	int REC_LEN1 = 30; 
	IndexScan iscan = null;
	BigT bigtable = null;
	BTreeFile btf;

	public static String[] ParseString(String toParse) {
		String letter=null;
		
		boolean type3Flag = false;
		boolean type1Flag = false;
		//Init a String array to be sent to the calling function
		String[] outString = new String[2];
		
		//Check for the * character
		if(toParse.equals('*'))
		{
			outString[0] = null;
			outString[1] = null;
			type1Flag = true;
		}
		else {
		//Check for the range characters
		int ToParseLength=toParse.length();
		int StringLengthCounter = 0;
		char tempString = 'a';
		String startIndex = null;
		String endIndex = null;
		boolean IndexChangeFlag = false;
		char[] LetterArray = new char[ToParseLength];
		
			for(int i=0; i < toParse.length(); i++) {
				tempString = toParse.charAt(i);
				if(i==0)
					if(tempString != '(')
						break;
					else 
						{
						type3Flag = true;
						continue;
						}
				else
				{
						if(IndexChangeFlag == false)
						{
							if(tempString != ',')
							{
								LetterArray[StringLengthCounter] = tempString;
								StringLengthCounter++;
							}
							else
							{
								IndexChangeFlag = true;
								StringLengthCounter = 0;
								outString[0] = String.valueOf(LetterArray).trim();
								LetterArray = new char[ToParseLength];
							}
						}
						else 
						{
							if(tempString != ')')
							{
								LetterArray[StringLengthCounter] = tempString;
								StringLengthCounter++;
							}
							else
							{
								outString[1] = String.valueOf(LetterArray).trim();
								break;
							}
						}
				}
			}
		}
		
		if (!type3Flag && !type1Flag){
		//This case is for equality search
		outString[0] = toParse;
		outString[1] = null;
		type3Flag = false;
		}
		
		 return outString;
	}
	
	public String[] unpadAttributes(String RL, String CL, String Value) {
		String[]ToUnPad= new String[3];
		ToUnPad[0] = RL.substring(2+Integer.parseInt(RL.substring(0,2)));
		ToUnPad[1] = CL.substring(2+Integer.parseInt(CL.substring(0,2)));
		ToUnPad[2] = Value.substring(2+Integer.parseInt(Value.substring(0,2)));
		return ToUnPad;
	}
	
	public int checkForType(String[] stringParse) {
		
		if(stringParse[0].contains("*")) {
		//	System.out.println("Stream.java - Type 1"); // Get all the elements
			return 0;
		}
		else if(stringParse[0] != null && stringParse[1] == null)
		{
		//	System.out.println("Stream.java - Type 2"); // Get the equality search
			return 1;
		}
		else
		{
		//	System.out.println("Stream.java - Type 3"); //Perform range search
			return 2;
		}
	}
	
	String btName = Integer.toString(BtreeCounter);
	
	//Constructor for the Streams
	public Stream(String filename, int orderType, String rowFilter, String columnFilter, String valueFilter) {
		
		//Assign bigt
		
		//BTreeName creation
		//btName = Integer.toString(orderType) + rowFilter.substring(0,2) + columnFilter.substring(0,2) + valueFilter.substring(0,2);
		
//		System.out.println("Stream - Line #199 row filter: " + rowFilter);
//		System.out.println("column filter: " + columnFilter);
//		System.out.println("value filter: " + valueFilter);
		
		//Parse the filters
		//Init the params
		String[] rowQueryStrings = new String[2];
		String[] columnQueryStrings = new String[2];
		String[] valueQueryStrings = new String[2];
		int rowType = 0;
		int columnType = 0;
		int valueType = 0;
		
		
		
		//init the flags to check if the records have to be inserted or not
		boolean RowFlag = false;
		boolean ColumnFlag = false;
		boolean ValueFlag = false;
		
		//Get the query parameter settings
		rowQueryStrings = ParseString(rowFilter);
		columnQueryStrings = ParseString(columnFilter);
		valueQueryStrings = ParseString(valueFilter);
		rowType = checkForType(rowQueryStrings);
		columnType = checkForType(columnQueryStrings);
		valueType = checkForType(valueQueryStrings);
		
		//Init temp scan params
		String RLTemp = null;
		String CLTemp = null;
		String ValueTemp = null;
		
		//Scan the database heapfile
		Scan scan = null;
		
		//Initialize the tuple that needs to be returned from getNext()
		Tuple t = null;
		  t = new Tuple();
	      AttrType[] attrType = new AttrType[4];
	      attrType[0] = new AttrType(AttrType.attrString);
	      attrType[1] = new AttrType(AttrType.attrString);
	      attrType[2] = new AttrType(AttrType.attrString);
	      attrType[3] = new AttrType(AttrType.attrInteger);
	      short[] attrSize = new short[4];
	      attrSize[0] = 30;
	      attrSize[1] = 30;
	      attrSize[2] = 50;
	      attrSize[3] = 16;
	      
	      
	      try {
	          t.setHdr((short) 4, attrType, attrSize);
	        }
	        catch (Exception e) {
	          e.printStackTrace();
	        }

	      //Get the size of the tuple
	      int size = t.size();
	      //System.out.println(Integer.toString(size));
	      
	      //Create a Tuple with the pre-defined size
	      t = new Tuple(size);
	  
	      
		//System.out.println("The number of fields:" + Integer.toString(t.noOfFlds()));
		
		//Init the Map structure with the headers
	      try {
	          t.setHdr((short) 4, attrType, attrSize);
	        }
	        catch (Exception e) {
	          e.printStackTrace();
	        }
	      
		
	
		    try {
		    	
		
		    Heapfile hf=new Heapfile(filename);
		    
		    System.out.println("Streams - line#282 Heapname: " + filename);
		  
		      scan = new Scan(hf); 
		    }
		    catch (Exception e) {
		      e.printStackTrace();
		    }
		    
		    RID rid = new RID();
		    
		    //Initial getNext
		    try {
		        t = scan.getNext(rid);
		      }
		    
		      catch (Exception e) {
		        e.printStackTrace();
		      }
		    try {
		    }
		    catch(Exception e)
		    {
		    	e.printStackTrace();
		    }
		    //Create a temp tuple to copy
		    Tuple temp = new Tuple(size);
		    
		    // Init the output array of strings
		    String[] UnpaddedStrings = new String[3];
		    
		    //Set the header for the tuple
		    try {
		          temp.setHdr((short) 4, attrType, attrSize);
		        }
		        catch (Exception e) {
		          e.printStackTrace();
		        }
		    
		    String key = null;
		    int max_len = 0;
		    int TimeStamp = 0;
		    
		    
		   
		    //Init the BTFile
			btf = null;
			String btreefilename = null;
			try {
				int temp2 = 0;
				btreefilename = readFile(filepath);
				//System.out.println(btreefilename);
				temp2 = Integer.parseInt(btreefilename);
				temp2++;
				btName = Integer.toString(temp2);
				btf = new BTreeFile("QueryProc" + btName, AttrType.attrString, REC_LEN1,0);
				writeBtreeName(btName, filepath);
				
				
			//btf = new BTreeFile("QueryProc" + 2);
			// ,AttrType.attrString, REC_LEN1,0
			
//			if(btf != null) {
//				btf.destroyFile();
//			}
//			btf = new BTreeFile("QueryProc",AttrType.attrString, REC_LEN1,0);
			
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		  
			
		    while ( t != null) {
		    	try {
		    
		    	
		    	temp.tupleCopy(t);
		
		    	RLTemp = temp.getStrFld(1);
		    	CLTemp = temp.getStrFld(2);
		    	ValueTemp = temp.getStrFld(3);
		    	TimeStamp = temp.getIntFld(4);
		    	
		    	
		    	UnpaddedStrings = unpadAttributes(RLTemp, CLTemp, ValueTemp);
		    	
		    	
		    	//Perform the checking for insertion into the heapfile that has to be used for querying
		    	
		    	//Perform the check for RL
		    	if(rowType == 0) {
		    		RowFlag = true;
		    	}
		    	else if(rowType == 1) {
		    		if(rowQueryStrings[0].compareTo(UnpaddedStrings[0]) == 0)
		    			RowFlag = true;
		    	}
		    	else if(rowType == 2){
		    		if((rowQueryStrings[0].compareTo(UnpaddedStrings[0]) <= 0) && rowQueryStrings[1].compareTo(UnpaddedStrings[0]) >= 0) {
		    			RowFlag = true;
		    		}
		    	}
		    	
		    	//Perform the check for CL
		    	if(columnType == 0) {
		    		ColumnFlag = true;
		    	}
		    	else if(columnType == 1) {
		    		if(columnQueryStrings[0].compareTo(UnpaddedStrings[1]) == 0)
		    			ColumnFlag = true;
		    	}
		    	else if(columnType == 2){
		    		if((columnQueryStrings[0].compareTo(UnpaddedStrings[1]) <= 0) && (columnQueryStrings[1].compareTo(UnpaddedStrings[1]) >= 0)) {
		    			ColumnFlag = true;
		    		}
		    	}
		    	
		    	//Perform the check for values
		    	if(valueType == 0) {
		    		ValueFlag = true;
		    	}
		    	else if(valueType == 1) {
		    		if(valueQueryStrings[0].compareTo(UnpaddedStrings[2]) == 0)
		    			ValueFlag = true;
		    	}
		    	else if(valueType == 2){
		    		if((Integer.parseInt(valueQueryStrings[0]) < Integer.parseInt(UnpaddedStrings[2])) && Integer.parseInt(valueQueryStrings[1]) >= Integer.parseInt((UnpaddedStrings[2]))) {
		    			ValueFlag = true;
		    		}
		    	}
		    	
		    	// Insert the record into the array if necessary
		    	String RowLabel = null;
		    	String ColumnLabel = null;
		    	
		    	if(UnpaddedStrings[0].length() > 5)
		    	{
		    		RowLabel = UnpaddedStrings[0].substring(0,5);
		    	}
		    	else
		    		RowLabel = UnpaddedStrings[0];
		    	
		    	if(UnpaddedStrings[1].length() > 5)
		    	{
		    		ColumnLabel = UnpaddedStrings[1].substring(0,5);
		    	}
		    	else
		    		ColumnLabel = UnpaddedStrings[1];
		    	
		    	String test = null;
		    	if(RowFlag == true && ColumnFlag == true && ValueFlag == true) {
			    	
		    		if(orderType == 1) {
		    			key = RowLabel + ColumnLabel + timeStampToString(TimeStamp);
		    		}
		    		else if(orderType == 2) {
		    			key = ColumnLabel + RowLabel + timeStampToString(TimeStamp);
		    		}
		    		else if(orderType == 3) {
		    			key = RowLabel + timeStampToString(TimeStamp);
		    		}
		    		else if(orderType == 4) {
		    			key = ColumnLabel + timeStampToString(TimeStamp);
		    		}
		    		else
		    			key = timeStampToString(TimeStamp);
		    		
		    		
		    		int key_length = key.length();
		    		if(key.length() > REC_LEN1)
		    		{
		    			test = key.substring(0,REC_LEN1);
		    		}
		    		else
		    			test = key;
		    		//Debug
		    		
		    		
		    		try {
		    		btf.insert(new StringKey(test),rid);
//		    		exception = false;
		    		}
		    		catch(Exception e) {
		    			e.printStackTrace();
		    			System.exit(0);
		    		}
		    		}
		    	
		    	t = scan.getNext(rid);
		    	
		    	RowFlag = false;
	    		ColumnFlag = false;
	    		ValueFlag = false;
		
	    		
		    	}
		    	catch (Exception e) {
		    		e.printStackTrace();
		    		System.exit(0);
		    	}
		    }
		
		    scan.closescan();
	}
	
			
	public Tuple getNext(RID rid,String filename) {
	
	String outval = null;
	Tuple t = null;
	t = new Tuple();
    AttrType[] attrType = new AttrType[4];
    attrType[0] = new AttrType(AttrType.attrString);
    attrType[1] = new AttrType(AttrType.attrString);
    attrType[2] = new AttrType(AttrType.attrString);
    attrType[3] = new AttrType(AttrType.attrInteger);
    short[] attrSize = new short[4];
    attrSize[0] = 30;
    attrSize[1] = 30;
    attrSize[2] = 50;
    attrSize[3] =16;
    
    try {
        t.setHdr((short) 4, attrType, attrSize);
      }
      catch (Exception e) {
        e.printStackTrace();
      }

    //Get the size of the tuple
    int size = t.size();
    //System.out.println(Integer.toString(size));
    
    //Create a Tuple with the pre-defined size
    t = new Tuple(size);
	
	//Init the Map structure with the headers
    try {
        t.setHdr((short) 4, attrType, attrSize);
      }
      catch (Exception e) {
        e.printStackTrace();
      }
    
	 //Scan through the BTree created
    ArrayList<String> cols = new ArrayList<String>();
    FldSpec[] projlist = new FldSpec[4];
    RelSpec rel = new RelSpec(RelSpec.outer); 
    projlist[0] = new FldSpec(rel, 1);
    projlist[1] = new FldSpec(rel, 2);
    projlist[2] = new FldSpec(rel, 3);
    projlist[3] = new FldSpec(rel, 4);
    
  
    if (iscan==null)
    {
	    try 
	    {
	      iscan = new IndexScan(new IndexType(IndexType.B_Index), filename , "QueryProc" + btName, attrType, attrSize, 4, 4, projlist, null, 4, false);
	    }
	    catch (Exception e) {
	      e.printStackTrace();
	    }
    }
    if(iscan != null)
    {
	    try
	    {
	    	 t = iscan.get_next();
	    }
	   
	    catch (Exception e) {
		      e.printStackTrace();
		    }
    }
    return t;
	}

public void closeStream() {
    	try {
    		System.out.println("Entered destroy");
    		btf = new BTreeFile("QueryProc" + btName, AttrType.attrString, REC_LEN1,0);
    	btf.destroyFile();
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
}
	
}



