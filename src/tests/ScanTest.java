package tests;

import heap.Tuple;
import heap.Scan;
import global.*;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import bigt.BigT;
import heap.Heapfile;
import tests.*;

public class ScanTest {
	
	RID rid = new RID();
	
	//Scan and test
			Scan scan = null;
			
			//Initialize the tuple that needs to be returned from getNext()
		public String MyScanTest (String heapName,int isFromJoin) throws IOException {
			
			int counter=0;
			
			heapName = heapName + "_heap.in";
			
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
		      
		      TestDriver tBatchInsert = new TestDriver ("BatchTest");
		      
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
		      
		      //System.out.println("The number of fields:" + Integer.toString(t.noOfFlds()));
		      
			//Get the heapfile for initializing the scan
			Heapfile hfTemp = null;
			try {
				hfTemp = new Heapfile(heapName);
				//System.out.println("Scan line65: " + heapName);
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
			        t = scan.getNext(rid);
			       
			      }
			   
			      catch (Exception e) {
			        e.printStackTrace();
			      }
//			    try {
//			    System.out.println(t.getStrFld(1));
//			    }
//			    catch(Exception e)
//			    {
//			    	e.printStackTrace();
//			    }
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
			    FileWriter csvWriter = null;
			    HashSet<String> hsrow = new HashSet<String>();
			    HashSet<String> hscolumn = new HashSet<String>();
			 
			    if(isFromJoin==1)
			    	csvWriter = new FileWriter("scan-output.csv");
			    while ( t != null) {
			    	try {
			    counter++;
			    	//System.out.println(Array.getLength(t.getTupleByteArray()));
			    	temp.tupleCopy(t);
			    	
			    	//System.out.println(temp.noOfFlds());
			    	if(isFromJoin==1) {
			    		System.out.println("Scan:" + "-" + temp.getStrFld(1).substring(2+Integer.parseInt(temp.getStrFld(1).substring(0,2))) + "-" +  temp.getStrFld(2).substring(2+Integer.parseInt(temp.getStrFld(2).substring(0,2))) + "-" + temp.getStrFld(3).substring(2+Integer.parseInt(temp.getStrFld(3).substring(0,2))) + "-" + temp.getIntFld(4));
				    	List<String> row = Arrays.asList(temp.getStrFld(1).substring(2+Integer.parseInt(temp.getStrFld(1).substring(0,2))), temp.getStrFld(2).substring(2+Integer.parseInt(temp.getStrFld(2).substring(0,2))), temp.getStrFld(3).substring(2+Integer.parseInt(temp.getStrFld(3).substring(0,2))),Integer.toString(temp.getIntFld(4)));
				    	csvWriter.append(String.join(",", row));
				        csvWriter.append("\n");
			    	}
			    	else {
			    		hsrow.add(temp.getStrFld(1).substring(2+Integer.parseInt(temp.getStrFld(1).substring(0,2))));
			    		hscolumn.add(temp.getStrFld(2).substring(2+Integer.parseInt(temp.getStrFld(2).substring(0,2))));
			    	}
			    	
			    	t = scan.getNext(rid);
			    	 //System.out.println("ScanTest: line no. 119" + Integer.toString(rid.slotNo) + " - " + Integer.toString(rid.pageNo.pid));
			    	}
			    	catch (Exception e) {
			    		e.printStackTrace();
			    		System.exit(1);
			    	}
			    }
			    
				System.out.println("ScanTest.java line89: #Maps scanned: " + counter + "and file write done successfully");
				 if(isFromJoin==0) {
				    	return "#Maps:"+counter+" #Distinct Rows:"+hsrow.size()+" #Distinct Cols:"+hscolumn.size();
				    }
			    
			    scan.closescan();
			    csvWriter.flush();
			    csvWriter.close();
			   
			   return "";
			}
		
		public RID getRid() {
			return this.rid;
		}
}
