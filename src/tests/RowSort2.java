package tests;

import java.util.Map;

import javax.sound.midi.SysexMessage;

import java.util.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.*;

import global.*;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.Sort;
import bigt.*;
import btree.WriteToFile;


public class RowSort2 implements GlobalConst{

	
	String out_filepath = "C:\\SR files\\College\\Subjects\\DBMSI\\Project\\Phase 3\\Demo version\\Group 4\\Minibase\\rowsort_output.csv";
	
	BigT outbigt = null;
	MID mid = new MID();
	String sort_filepath = "C:\\SR files\\College\\Subjects\\DBMSI\\Project\\Phase 3\\Demo version\\Group 4\\Minibase\\src\\tests\\rowsort.txt";
	
	//Create a hashmap for mapping to output heapfile
	//Map<Integer, List<RID>> data = new TreeMap<Integer, List<RID>>();
	Map<Integer, List<RID>> data = new TreeMap<Integer, List<RID>>();
	List<RID> listRid = new ArrayList<RID>();
	
	// Number of pages 
	int SORTPGNUM = 25; 
	
	
	//heapFileName = heapFileName + "_heap.in";
	
	//Record length
	int REC_LEN1 = 30;
	
	//Display flags
	boolean displaySort = false;
	boolean displayFileScan = false;
	boolean displayScan = false;
	boolean displayFileScan2 = false;
	boolean displayRowSort = false;
	boolean enable_debug_counter = true;
	boolean insert_nr_bigt = false;
	boolean write_to_file = false;
	
	//Scan pre-requisites
	RID rid = new RID();
	Scan scan = null;

	RID rid_temp = new RID();
	
//Get the values from the encoded strings
public String getValString(String str, int offset) {
	return str.substring(offset,offset+MAPSTRINGIDX);
}
	
public RowSort2(String heapFileName, String outBigTName, String columnFilter)
{
	String[] outval = new String[2];
	int outvalInt = 0;
	bigt.Map m = new bigt.Map();
	
	heapFileName = heapFileName + "s_heap.in";
	
	//Setup the tuple to accept the values
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
	
	
	Heapfile hf = null;
	//Open the existing heapfile
    try {
        hf = new Heapfile(heapFileName);
      }
      catch (Exception e) {
        e.printStackTrace();
      }
	
    String columnFilt = m.converttofixed(columnFilter, 30);
    
	 //Scan through the BTree created
    ArrayList<String> cols = new ArrayList<String>();
    FldSpec[] projlist = new FldSpec[4];
    RelSpec rel = new RelSpec(RelSpec.outer); 
    projlist[0] = new FldSpec(rel, 1);
    projlist[1] = new FldSpec(rel, 2);
    projlist[2] = new FldSpec(rel, 3);
    projlist[3] = new FldSpec(rel, 4);
    
    //Setup condition for filescan
    // set up an identity selection
    CondExpr[] expr = new CondExpr[2];
    expr[0] = new CondExpr();
    expr[0].op = new AttrOperator(AttrOperator.aopEQ);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].type2 = new AttrType(AttrType.attrString);
    expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
    expr[0].operand2.string = columnFilt;
    
    
    expr[0].next = null;
    expr[1] = null;
    
	//Perform a filescan using the iterator having a column filter equality condition check
    FileScan fscan = null;
    System.out.println("Rowsort2 lin151: " + heapFileName);
    
    try {
      fscan = new FileScan(heapFileName, attrType, attrSize, (short) 4, 4, projlist, expr);
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  
    if(displayFileScan)
    {
    String columnFiltDisp = unpadAttributes(columnFilt);
    String[] outval_str_fs = new String[3];
    int outval_ts_fs = 0;
    System.out.println("Column filter: " + columnFiltDisp);
    
//  get the values from file scan
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
    
    //perform a sorting operation on the scanned file
    
    //Check if the heapfile already exists
//    try {
//    	
//    Heapfile hf2 = new Heapfile("tempHeapFile");
//    System.out.println("Record count - line 141: " + hf2.getRecCnt());
//    }
//    catch(Exception e)
//    {
//    	e.printStackTrace();
//    }
//  
   // System.exit(1);
    }
    
    Sort sort = null;
    
    {
    try {
      sort = new Sort(attrType, (short) 4, attrSize, fscan, 1, order[0], REC_LEN1, SORTPGNUM);
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    }
    
    if(displaySort)
    {
    System.out.println("\n\n*****************************\n");
    System.out.println("Sorted file entries: \n");
    
    //test scanning through the heapfile
    System.out.println("Rowsort - line171: " + sort.getFileName());
    }
   
    try {
    t = sort.get_next();
    outvalInt = t.getIntFld(4);
    }
    catch(Exception e) {
    	e.printStackTrace();
    }
    if(displaySort)
    	System.out.println("Rowsort -Line 150 :" + outvalInt);
    
    //  Scan through the sorted stream

	while (t != null) {
		
	    try {
	    outvalInt = t.getIntFld(4);
	    outval[0] = t.getStrFld(1);
	    outval[1] = t.getStrFld(2);
		t = sort.get_next();
		if(displaySort)
			System.out.println("RowSort - line124 :" + " - RL: " + outval[0] + "  TimeStamp:" + outvalInt + " CL: " + outval[1]);
	    }
	    catch (Exception e) {
		e.printStackTrace();
	    }
	  }
if(displaySort)
	System.out.println("\n\n Scanning...");


//String rowFilt = m.converttofixed("Vatican_City", 30);
//
////Display the rows containing the remaining columns
////Setup condition for filescan
//  // set up an identity selection
//  //CondExpr[] expr = new CondExpr[2];
//expr = new CondExpr[3]; 
//expr[0] = new CondExpr();
//expr[0].op = new AttrOperator(AttrOperator.aopEQ);
//expr[0].type1 = new AttrType(AttrType.attrSymbol);
//expr[0].type2 = new AttrType(AttrType.attrString);
//expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
//expr[0].operand2.string = rowFilt;
//expr[0].next = null;
//expr[1] = new CondExpr();
//expr[1].op = new AttrOperator(AttrOperator.aopNE);
//expr[1].type1 = new AttrType(AttrType.attrSymbol);
//expr[1].type2 = new AttrType(AttrType.attrString);
//expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
//expr[1].operand2.string = columnFilt;
//expr[1].next = null;
//expr[2] = null;
//  
//	//Perform a filescan using the iterator having a column filter equality condition check
//  FileScan fscan_out = null;
//  
//  try {
//    fscan_out = new FileScan(heapFileName, attrType, attrSize, (short) 4, 4, projlist, expr);
//  }
//  catch (Exception e) {
//    e.printStackTrace();
//  }
//
//  if(displayFileScan2)
//  {
//  String columnFiltDisp = unpadAttributes(columnFilt);
//  String[] outval_str_fs = new String[3];
//  int outval_ts_fs = 0;
//  System.out.println("Column filter: " + columnFiltDisp);
//  
////  get the values from file scan
//  try {
//      t = fscan_out.get_next();
//      }
//      catch(Exception e) {
//      	e.printStackTrace();
//      }
//      //Scan through the sorted stream
//  	while (t != null) {
//  		
//  	    try {
//  	    outvalInt = t.getIntFld(4);
//  	    outval_str_fs[0] = t.getStrFld(1);
//  	    outval_str_fs[1] = t.getStrFld(2);
//  	    outval_str_fs[2] = t.getStrFld(3);
//  		t = fscan_out.get_next();
//  		System.out.println("\nFileScan - <" + " RL: " + outval_str_fs[0] + " ; CL: " + outval_str_fs[1] + " ; Val: " + outval_str_fs[2] + " ; TS: " + outval_ts_fs + ">");
//  	    }
//  	    catch (Exception e) {
//  		e.printStackTrace();
//  	    }
//  	  }
//  
//  //perform a sorting operation on the scanned file
//  
//  //Check if the heapfile already exists
////  try {
////  	
////  Heapfile hf2 = new Heapfile("tempHeapFile");
////  System.out.println("Record count - line 141: " + hf2.getRecCnt());
////  }
////  catch(Exception e)
////  {
////  	e.printStackTrace();
////  }
////  
//  //System.exit(1);
//  }
    


	
	String heapName = sort.getFileName();
	
	//Previous iter and cur RL values
	String prevRL = null;
	String curRL = null;
	
	int counter=0;
	
	t = null;
	  t = new Tuple();
      attrType = new AttrType[4];
      attrType[0] = new AttrType(AttrType.attrString);
      attrType[1] = new AttrType(AttrType.attrString);
      attrType[2] = new AttrType(AttrType.attrString);
      //attrType[3] = new AttrType(AttrType.attrString);
      attrType[3] = new AttrType(AttrType.attrInteger);
      attrSize = new short[4];
      attrSize[0] = 30;
      attrSize[1] = 30;
      attrSize[2] = 50;
      
      TestDriver tBatchInsert = new TestDriver ("BatchTest");
      
      try {
          t.setHdr((short) 4, attrType, attrSize);
        }
        catch (Exception e) {
          e.printStackTrace();
        }

      //Get the size of the tuple
      size = t.size();
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
      
      //System.out.println("The number of fields:" + Integer.toString(t.noOfFlds()));
      
	//Get the heapfile for initializing the scan
	Heapfile hfTemp = null;
	try {
		hfTemp = new Heapfile(heapName);
		System.out.println("Scan line65: " + heapName);
	}
	catch (Exception e)
	{
		e.printStackTrace();
	}
	//String[] testScan = new String[49999];
	    try {
	      scan = new Scan(hfTemp);
	    }
	    catch (Exception e) {
	      e.printStackTrace();
	    }
	    
	    
	    
	    //Initial getNext
	    try {
	    	rid_temp = scan.getCurrentRid();
	        t = scan.getNext(rid);
	        
 	      }
	   
	      catch (Exception e) {
	        e.printStackTrace();
	      }
	    
	    
	    
//	    try {
//	    System.out.println(t.getStrFld(1));
//	    }
//	    catch(Exception e)
//	    {
//	    	e.printStackTrace();
//	    }
	    //System.out.println(Integer.toString(size));
	    //Create a temp tuple to copy
	    Tuple temp = new Tuple(size);
	    
	    //Set the header for the tuple
	    try {
	          temp.setHdr((short) 4, attrType, attrSize);
	        }
	        catch (Exception e) {
	          e.printStackTrace();
	        }
	    
	    int count_debug=0;
	    int timeStamp = 0;
	    String valueRecent = null;

//cluster_start
while ( t != null) {
	
	    	try {
	    counter++;
	    	//System.out.println(Array.getLength(t.getTupleByteArray()));
	    	
	    
	    	
	    	//System.out.println(temp.noOfFlds());
	    	if(displayScan)
	    		System.out.println("ScanTest:" + " - " + Integer.toString(counter) + " - " + temp.getStrFld(1) +  temp.getStrFld(2) + temp.getStrFld(3) + temp.getIntFld(4));
	        
	    	temp.tupleCopy(t);
	    	
// comment1
	    	if(rid_temp == null)
	    	{
	    		//rid_temp = scan.getCurrentRid();
	    		//t=scan.getNext(rid);
	    		rid_temp = scan.getCurrentRid();
//	    		rid_temp.pageNo.pid += 1;
//	    		rid_temp.slotNo = 1;
	    		if(displayScan)
	    		{
	    		System.out.println("Rid : pageno - pid: " + rid_temp.pageNo.pid);
	    		System.out.println("Rid : pageno - slot: " + rid_temp.slotNo);
	    		}
	    	}
	    	
	    	
	    	
	    	
//	    	prevRL = temp.getStrFld(1);
	    	if(displayScan)
	    	 System.out.println("ScanTest: line no. 119" + Integer.toString(rid.slotNo) + " - " + Integer.toString(rid.pageNo.pid));
	    	}	    	
	    	catch (Exception e) {
	    		e.printStackTrace();
	    		System.exit(1);
	    	}
	    	
	    	
	    	
//	    	temp.tupleCopy(t);
	    	
	    	
	    	
	    	
	    	try {
	    	    curRL = temp.getStrFld(1);
	    	    }
	    	    catch(Exception e)
	    	    {
	    	    	e.printStackTrace();
	    	    }
//	    	try {
//	    	timeStamp = temp.getIntFld(4);
//	    	}
//	    	catch(Exception e)
//	    	{
//	    		e.printStackTrace();
//	    	}
	    	
	    	
	    	RID prev_rid = new RID();
	    	RID cur_rid = new RID();
	    	int iteration = 0;
	    	
	    	if(displayScan)
	    		System.out.println("\nCur RL: "+curRL+"\n Prev RL: " + prevRL);
	    	
	    	
	    	
	    	
	    	    //Check if the cluster has surpassed
	    	    if(prevRL == null || prevRL.equals(curRL))
	    	    {
	    	    	iteration++;
	    	    	
	    	    	System.out.println("Rowsort2 - line520: prevRL - " + prevRL + "-" + valueRecent + "-" + timeStamp + "-" + rid_temp);
	    	    	//System.out.println("RowSort - line383: " + Arrays.asList(data));
	    	    	
	    	    	if(iteration == 2)
	    	    		prev_rid = rid_temp;
	    	    	cur_rid = rid_temp;
	    	    	listRid.add(rid_temp);
	    	    }
	    	    else
	    	    {
	    	    	iteration = 0;
	    	    	
	    	    	System.out.println("Rowsort2 - line523: prevRL - " + prevRL + "-" + valueRecent + "-" + timeStamp);
	    	    	if(displayScan)
	    	    		System.out.println("\nRowSort - line372: - List " + listRid);
	    	    	
	    	    	
	    	    	//System.out.println("rowsort2 - line543: " + rid_temp + "-" + cur_rid);
//	    	    	if(cur_rid == rid_temp)
//	    	    	{
//	    	    		listRid.remove(3);
//	    	    		listRid.add(prev_rid);
//	    	    	}
	    	    	
	    	    	//data.put(timeStamp, listRid);
	    	    	System.out.println("rowsort2 - line530: " + listRid);
	    	    	data.put(Integer.parseInt(valueRecent), listRid);
	    	    	System.out.println("RowSort2 line532: " + "**************");
	    	    	//listRid = new ArrayList<RID>();
	    	    	if(displayScan)
	    	    		System.out.println("\nRowSort - line383: " + Arrays.asList(data));
	    	    	//listRid.clear();
	    	    	listRid = new ArrayList<RID>();
	    	    	listRid.add(rid_temp);
	    	    }
	    	    
	    	    
	    	    
	    	    
	    	    
//comment2	    	    
	    	    if(rid_temp == null)
	    	    {
	    	    	try {
	    	    	//data.put(timeStamp, listRid);
	    	    	System.out.println("Rowsort2 - line546: prevRL - " + prevRL + "-" + valueRecent);
	    	    	data.put(Integer.parseInt(valueRecent), listRid);
	    	    	listRid = new ArrayList<RID>();
	    	    	break;
	    	    	}
	    	    	catch(Exception e)
	    	    	{
	    	    		e.printStackTrace();
	    	    	}
	    	    }
	    	    if(displayScan)
	    	    {
	    	    	System.out.println("\nRowSort - count: " + count_debug);
	    	    	System.out.println("\nRowSort - line376: pid - " + rid_temp.pageNo.pid + " - " + rid_temp.slotNo);
	    	    }
	    	    //System.exit(1);	
	    
	    	    
	    	    try {
	    	    prevRL = temp.getStrFld(1);
	    	    timeStamp = temp.getIntFld(4);
	    	    valueRecent = temp.getStrFld(3);
	    	    rid_temp = scan.getCurrentRid();
	    	    if(displayScan)
	    	    {
	    	    System.out.println("\nRowSort - line372: - TimeStamp " + timeStamp);
	    	    System.out.println("\nRowSort - line373: - value " + valueRecent);
	    	    System.out.println("\n**********************************************************************************");
	    	    }
	    	    t = scan.getNext(rid);
	    	    }
	    	    catch(Exception e)
	    	    {
	    	    	e.printStackTrace();
	    	    }
	    	    
	    	    
	    	    if(enable_debug_counter)
	    	    {
		    	    if(count_debug == 100)
		    	    {
		    	    	try {
		    	    	System.out.println("Rowsort2 - line591: - " + prevRL + "-" + valueRecent);
		    	    	data.put(Integer.parseInt(valueRecent), listRid);
		    	    	
		    	    	}
		    	    	catch(Exception e)
		    	    	{
		    	    		e.printStackTrace();
		    	    	}
		    	    	break;
		    	    }
		    	    
		    	    count_debug++;
		    	   }
				}
	    
	    if(!listRid.isEmpty())
	    {
	    	if(displayScan)
	    	{
	    	System.out.println("RowSort - line456: " + timeStamp);
	    	System.out.println("RowSort - line457: " + valueRecent);
	    	}
	    	//data.put(timeStamp, listRid);
	    	data.put(Integer.parseInt(valueRecent), listRid);
	    }
	    
	    String output_str = null;
	    String[] outval_str_rs = new String[3];
	    int outvalInt_rs = 0;
	    
	    String rowFilt = null;
	    int counter2=0;
	    
	    System.out.println("\n\n***************************************************\n*********************************************\n**************************************************");
	    
	    //Sort the generated list of RIDs with the latest values
	    //for (Map.Entry<Integer, List<RID>> entry : data.entrySet()) 
	    for (Map.Entry<Integer, List<RID>> entry : data.entrySet())
	    {
	    	if(displayScan)
	    		System.out.println(  "[" + entry.getKey() + ", " + entry.getValue() + "]"); 
	    	for (int i=0; i < entry.getValue().size(); i++)
	    	{
	    		//System.out.println("\nRowSort - line469: " + entry.getValue().get(i));
	    		rid_temp = entry.getValue().get(i);
	    		try {
	    	    	
	    			if(rowFilt != outval_str_rs[0])
	    				rowFilt = m.converttofixed(outval_str_rs[0], 30);
	    			
	    	        t = hfTemp.getRecord(rid_temp);
	    	        temp.tupleCopy(t);
	    	        outval_str_rs[0] = temp.getStrFld(1);
	    	        outval_str_rs[1] = temp.getStrFld(2);
	    	        outval_str_rs[2] = temp.getStrFld(3);
	    	        outvalInt_rs = temp.getIntFld(4);
	    	        //insertIntoBigT(outval_str_rs, outvalInt_rs, outBigTName);
	    	        System.out.println("Row sort - Inserting record: " + outval_str_rs[0] + "-" + outval_str_rs[1] + "-" + outval_str_rs[2] + "-" + outvalInt_rs);
	    	        counter++;
	    	        
	    	        output_str = outval_str_rs[0] + "," + outval_str_rs[1] + "," + outval_str_rs[2] + "," + outvalInt_rs;
	    	  		
	    	        if(write_to_file)
	    	        	writesortFileName(output_str, out_filepath);
	    	        
	    	      }
	    	      catch (Exception e) {
	    	        e.printStackTrace();
	    	      }
	    		
	    		//System.out.println("Inserting record - <" + " RL: " + outval_str_rs[0] + " ; CL: " + outval_str_rs[1] + " ; Val: " + outval_str_rs[2] + " ; TS: " + outvalInt_rs + ">");
	    		
	    		if(displayScan) {
	    		System.out.println("\nRowSort - line474: - RL:" + outval_str_rs[0]);
	    		System.out.println("RowSort - line475: - value:" + outval_str_rs[1]);
	    		System.out.println("RowSort - line476: - TS:" + outvalInt_rs);
	    		}
	    		//System.out.println("*******************");
	    		
	    		
	    	}
	    	
	    	//Get the remaining records for that column
	    	
	    	//columnFilt = m.converttofixed(outval_str_rs[1], 30);

	    	//Display the rows containing the remaining columns
	    	//Setup condition for filescan
	    	  // set up an identity selection
	    	  //CondExpr[] expr = new CondExpr[2];
	    	expr = new CondExpr[3]; 
	    	expr[0] = new CondExpr();
	    	expr[0].op = new AttrOperator(AttrOperator.aopEQ);
	    	expr[0].type1 = new AttrType(AttrType.attrSymbol);
	    	expr[0].type2 = new AttrType(AttrType.attrString);
	    	expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
	    	expr[0].operand2.string = rowFilt;
	    	expr[0].next = null;
	    	expr[1] = new CondExpr();
	    	expr[1].op = new AttrOperator(AttrOperator.aopNE);
	    	expr[1].type1 = new AttrType(AttrType.attrSymbol);
	    	expr[1].type2 = new AttrType(AttrType.attrString);
	    	expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
	    	expr[1].operand2.string = columnFilt;
	    	expr[1].next = null;
	    	expr[2] = null;
	    	  
	    		//Perform a filescan using the iterator having a column filter equality condition check
	    	  FileScan fscan_out = null;
	    	  
	    	  try {
	    	    fscan_out = new FileScan(heapFileName, attrType, attrSize, (short) 4, 4, projlist, expr);
	    	  }
	    	  catch (Exception e) {
	    	    e.printStackTrace();
	    	  }
	    	  
	    	  String columnFiltDisp = unpadAttributes(columnFilt);
	    	  String[] outval_str_fs = new String[3];
	    	  int outval_ts_fs = 0;
	    	  //System.out.println("Column filter: " + columnFilt + "Row filter: " + rowFilt);
	    	  
	    	//  get the values from file scan
	    	  try {
	    	      t = fscan_out.get_next();
	    	      }
	    	      catch(Exception e) {
	    	      	e.printStackTrace();
	    	      }
	    	  
	    	  
	    	      //Scan through the sorted stream
	    	  	while (t != null) {
	    	  		
	    	  	    try {
	    	  	    outvalInt = t.getIntFld(4);
	    	  	    outval_str_fs[0] = t.getStrFld(1);
	    	  	    outval_str_fs[1] = t.getStrFld(2);
	    	  	    outval_str_fs[2] = t.getStrFld(3);
	    	  		t = fscan_out.get_next();
	    	  		
	    	  		output_str = outval_str_fs[0] + "," + outval_str_fs[1] + "," + outval_str_fs[2] + "," + outvalInt;
	    	  		
	    	  		if(write_to_file)
	    	  			writesortFileName(output_str, out_filepath);
	    	  		
	    	  		if(insert_nr_bigt)
	    	  		//insertIntoBigT(outval_str_fs, outvalInt, outBigTName);
	    	  		 //System.out.println("Row sort - Inserting record: " + counter);
		    	     counter++;
		    	     if(displayFileScan2)
		    	    	 System.out.println(" || <" + " RL: " + outval_str_fs[0] + " ; CL: " + outval_str_fs[1] + " ; Val: " + outval_str_fs[2] + " ; TS: " + outval_ts_fs + ">");
	    	  	    }
	    	  	    catch (Exception e) {
	    	  		e.printStackTrace();
	    	  	    }
	    	  	  }
	    	  
	    	  //perform a sorting operation on the scanned file
	    	  
	    	  //Check if the heapfile already exists
	    	//  try {
	    	//  	
	    	//  Heapfile hf2 = new Heapfile("tempHeapFile");
	    	//  System.out.println("Record count - line 141: " + hf2.getRecCnt());
	    	//  }
	    	//  catch(Exception e)
	    	//  {
//	    	  	e.printStackTrace();
	    	//  }
	    	//  
	    	  //System.exit(1);
	    	  }
	    	
	    	System.out.println("**********************************************************");

		//System.out.println("ScanTest.java line89: #Maps scanned: " + counter);
	    
	    scan.closescan();
	    try {
			ScanTest st= new ScanTest () ;
			st.MyScanTest(outBigTName,1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
}


//Function to unpad the saved attributes
public String unpadAttributes(String RL) {
	String ToUnPad= null;
	ToUnPad = RL.substring(2+Integer.parseInt(RL.substring(0,2)));
	return ToUnPad;
}

public void insertIntoBigT(String[] fields, int timestamp, String outbigtName)
{
	String big_exist_check = null;
	if(outbigt == null)
	{
	try{
		outbigt = new BigT(outbigtName, 1);
		big_exist_check = readFile(sort_filepath);
		if(big_exist_check.equals(outbigtName))
		{
		outbigt.deleteBigt(outbigtName, 1);
		outbigt = new BigT(outbigtName, 1);
		}
		else
			writesortFileName(outbigtName, sort_filepath);
	}
	catch(Exception e) {
		e.printStackTrace();
	}
	}
		String[] temp = null;

			bigt.Map map = new bigt.Map();
			
			//System.out.println("rowsort line789 - Written values: " + fields[0] + "-" + fields[1] + "-" + fields[2] + "-" + timestamp);
			
			//Map to Tuple implementation
			map.setRowLabel(map.converttofixed(fields[0],30));
			map.setColumnLabel(map.converttofixed(fields[1],30));
			map.setValue(map.converttofixed(fields[2],50));
			map.setTimeStamp(timestamp);


		
			try {
				mid = outbigt.insertMap(map);
			}
				
			catch(Exception e)
			{
				e.printStackTrace();
			}
			
			System.out.flush();
			

}

public void writesortFileName(String dataWrite, String filepath) {
	WriteToFile data = new WriteToFile(filepath, true);
	try {
	data.WriteFile(dataWrite);
	//System.out.println("BTreeName written...");
	}
	catch(Exception e) {
		e.printStackTrace();
	}
}

public String readFile (String filepath) { 
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

}
