package tests;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Scanner;

import bigt.BigT;
import bigt.Map;
import diskmgr.PCounter;
import global.AttrType;
import global.GlobalConst;
import global.MID;
import global.SystemDefs;
import global.TupleOrder;
import heap.Heapfile;
import heap.Tuple;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.Sort;

public class BatchInsert extends TestDriver
implements GlobalConst{
	
	//Skip insert
	boolean skip_insert = true;
	boolean displayScan = false;
	
	// Number of pages 
			int SORTPGNUM = 25; 
			
			//heapFileName = heapFileName + "_heap.in";
			
			//Record length
			int REC_LEN1 = 16;
	
public BigT batchinsert(String fileName, int bigtType, String bigtName){
	
			
		TestDriver tBatchInsert = new TestDriver ("phase");
		
		int counter = 0;
		
	
		BigT bigt = null;
		try{
			bigt = new BigT(bigtName, bigtType);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	
		System.out.println("file name"+fileName);
		File file = new File(fileName);
		String[][] values = new String[49999][4];
		int count = 0;
		
		MID mid=null;
		try {
			String[] temp = null;
			Scanner inputStream = new Scanner(file);
			while(inputStream.hasNext()){
				String data = inputStream.next();
				data = data.replaceAll("[^\\x00-\\x7F]", "");
	
				temp = data.split(",");
				Map map = new Map();
				
				counter++;
				
				//Map to Tuple implementation
				map.setRowLabel(map.converttofixed(temp[0],30));
				map.setColumnLabel(map.converttofixed(temp[1],30));
				map.setValue(map.converttofixed(temp[2],50));
				map.setTimeStamp(Integer.parseInt(temp[3]));


				if (skip_insert)
				{
				try {
					mid = bigt.insertMap(map);
				}
					
				catch(Exception e)
				{
					e.printStackTrace();
				}
				}
				
				System.out.flush();
				
				
				System.out.println(counter);
			}
			
			inputStream.close();
			
		}
		catch (FileNotFoundException e){
			e.printStackTrace();
		}

		try {
	         FileOutputStream fileOut =
	         new FileOutputStream(bigtName+".txt");
	         ObjectOutputStream out = new ObjectOutputStream(fileOut);
	         out.writeObject(bigt.getHashMap());
	         out.close();
	         fileOut.close();
	         //System.out.printf("Serialized data is saved in"+bigtName+".txt");
	      } 
		catch (IOException i) {
	         i.printStackTrace();
	      }

		
		bigt.closeBTree();
		
		System.out.println("Rcounter"+Integer.toString(PCounter.rcounter));
		System.out.println("Wcounter"+Integer.toString(PCounter.wcounter));
		
		//sortFile(bigtName);
		
		return bigt;
		
	}

public void sortFile(String heapFileName)
{
	
	Tuple t = null;
	t = new Tuple();
	AttrType[] attrType = new AttrType[4];
	attrType[0] = new AttrType(AttrType.attrString);
	attrType[1] = new AttrType(AttrType.attrString);
	attrType[2] = new AttrType(AttrType.attrString);
	attrType[3] = new AttrType(AttrType.attrInteger);
	short[] attrSize = new short[4];
	TupleOrder[] order = new TupleOrder[1];
	order[0] = new TupleOrder(TupleOrder.Ascending);
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
    
	//Perform a filescan using the iterator having a column filter equality condition check
    FileScan fscan = null;
    
    String heapNameSorted = null;
    BigT bigt2 = null;
	try{
		bigt2 = new BigT(heapFileName + "s", 1);
	}
	catch(Exception e) {
		e.printStackTrace();
	}
    
	heapFileName = heapFileName + "_heap.in";
	System.out.println("BatchINsert line187: " + heapFileName);
	
    try {
      fscan = new FileScan(heapFileName, attrType, attrSize, (short) 4, 4, projlist, null);
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    
    
    if(displayScan)
    {
    String[] outval_str_fs = new String[3];
    Integer outval_ts_fs = 0;
    
    try {
        t = fscan.get_next();
        }
        catch(Exception e) {
        	e.printStackTrace();
        }
        //Scan through the sorted stream
    	while (t != null) {
    		
    	    try {
    	    outval_ts_fs = t.getIntFld(4);
    	    outval_str_fs[0] = t.getStrFld(1);
    	    outval_str_fs[1] = t.getStrFld(2);
    	    outval_str_fs[2] = t.getStrFld(3);
    		t = fscan.get_next();
    		System.out.println("\nFileScan - <" + " RL: " + outval_str_fs[0] + " ; CL: " + outval_str_fs[1] + " ; Val: " + outval_str_fs[2] + " ; TS: " + outval_ts_fs + ">");
    	    }
    	    catch (Exception e) {
    		e.printStackTrace();
    	    }
    	  }
    }
    Map map = new Map();
    Sort sort = null;
    
    {
    try {
      sort = new Sort(attrType, (short) 4, attrSize, fscan, 4, order[0], REC_LEN1, SORTPGNUM);
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    }
   
    Integer outvalInt = 0;
    
    try {
    t = sort.get_next();
    outvalInt = t.getIntFld(4);
    }
    catch(Exception e) {
    	e.printStackTrace();
    }
    
    //  Scan through the sorted stream
    String[] outval = new String[3];

	while (t != null) {
		
	    try {
	    outvalInt = t.getIntFld(4);
	    outval[0] = t.getStrFld(1);
	    outval[1] = t.getStrFld(2);
	    outval[2] = t.getStrFld(3);
		t = sort.get_next();
	    
		//System.out.println("RowSort - line124 :" + " - RL: " + outval[0] + "  TimeStamp:" + outvalInt + " CL: " + outval[1]);
	    
	  //Map to Tuple implementation
		map.setRowLabel(map.converttofixed(outval[0],30));
		map.setColumnLabel(map.converttofixed(outval[1],30));
		map.setValue(map.converttofixed(outval[2],50));
		map.setTimeStamp(outvalInt);
		
		try {
			bigt2.insertMap(map);
		}
			
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		
	    }
	    catch (Exception e) {
		e.printStackTrace();
	    }
	  }
	
	System.out.println("Filesort successful...");
	
	}
}

