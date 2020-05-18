package tests;

import java.io.IOException;

import bigt.Map;
import global.*;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.SpaceNotAvailableException;
import heap.Tuple;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.Sort;
import iterator.SortException;

public class RowJoin implements GlobalConst {
	
	
public void removeDup(AttrType[] attrType, short[] attrSize, FileScan f,String i) {
		
		boolean displaySort = true;
		
		 
			
			
			
			//Setup the tuple to accept the values
			Tuple t = null;
			t = new Tuple();
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
		
    	Sort sort = null;
    	
    	TupleOrder[] order = new TupleOrder[1];
    	order[0] = new TupleOrder(TupleOrder.Ascending);
    	
    	// Number of pages 
    	int SORTPGNUM = 25; 
    	
    	//Record length
    	int REC_LEN1 = 30;
    	
    	
    	
        if(displaySort)
        {
        try {
          sort = new Sort(attrType, (short) 4, attrSize, f, 1, order[0], REC_LEN1, SORTPGNUM);
        }
        catch (Exception e) {
          e.printStackTrace();
        }
        }
        
        try {
           
            t = sort.get_next();
            }
            catch(Exception e) {
            	e.printStackTrace();
            }
           
        String[] outval = new String[3];
    	int outvalInt = 0;
    	String prev = "";
    	Tuple temp = new Tuple(size);
	    
	    //Set the header for the tuple
	    try {
	          temp.setHdr((short) 4, attrType, attrSize);
	        }
	        catch (Exception e) {
	          e.printStackTrace();
	        }
	    	Heapfile hf3 = null;
	
		//Open the existing heapfile
	    try {
	        hf3 = new Heapfile("sortedheapfile"+i+"_heap.in");
	      }
	    catch(Exception e)
	    {
	    	e.printStackTrace();
	    }
            
            //  Scan through the sorted stream
            if(displaySort)
            {
        	while (t != null) {
        		
        	    try {
        	    
        	    	
        	    outvalInt = t.getIntFld(4);
        	    outval[0] = t.getStrFld(1);
        	    outval[1] = t.getStrFld(2);
        	    outval[2] = t.getStrFld(3);
        	    
        	  
        	    
        		//System.out.println("RowJoin - line189 :" + " - RL: " + outval[0] + " CL: " + outval[1] + " Value: "+ outval[2]+"  TimeStamp:" + outvalInt );
        	    if(!outval[0].equals(prev) &&  !prev.equals(""))
        	    {
        	    	//byte[] mapPtr = m.copyMapToTuple(m);
        	    	
        	    	Map map = new Map();
    				
    				
    				//Map to Tuple implementation
    				map.setRowLabel(map.converttofixed(temp.getStrFld(1),30));
    				map.setColumnLabel(map.converttofixed( temp.getStrFld(2),30));
    				map.setValue(map.converttofixed(temp.getStrFld(3),50));
    				map.setTimeStamp(temp.getIntFld(4));
    				byte[] rec = map.copyMapToTuple(map);
        	   
        	    	//System.out.println("Row Join Line 125"+rec);
        	    	hf3.insertMap(rec);
        	    }
        	    //System.out.println("RowJoin - line189 :" + " - RL: " + temp.getStrFld(1) + " CL: " + temp.getStrFld(2) + " Value: "+ temp.getStrFld(3)+"  TimeStamp:" + temp.getIntFld(4) );
        	    
        	    prev = outval[0];
        		temp.tupleCopy(t);
        	    t = sort.get_next();
        	    
        	
        	    }
        	    catch (Exception e) {
        		e.printStackTrace();
        	    }
        	    
        	  
        	    
        	  }
        	try {
				sort.close();
			} catch (SortException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	
        	

}
		
		
	}
	
	public RowJoin(String heapFileName1, String heapFileName2,String bigtName, String columnFilter)
	{
		
		
		Heapfile hf1 = null;
		Heapfile hf2 = null;
		
//		heapFileName1 = heapFileName1 + "_heap.in";
//		heapFileName2 = heapFileName2 + "_heap.in";
		
		//Open the existing heapfile
	    try {
	        hf1 = new Heapfile(heapFileName1);
	        hf2 = new Heapfile(heapFileName2);
	      }
	      catch (Exception e) {
	        e.printStackTrace();
	      }
	    
	   
		
	    AttrType[] attrType = new AttrType[4];
		attrType[0] = new AttrType(AttrType.attrString);
		attrType[1] = new AttrType(AttrType.attrString);
		attrType[2] = new AttrType(AttrType.attrString);
		attrType[3] = new AttrType(AttrType.attrInteger);
		
		short[] attrSize = new short[4];
		attrSize[0] = 30;
		attrSize[1] = 30;
		attrSize[2] = 50;
	    
	    bigt.Map m = new bigt.Map();
	    
	    
	    String columnFilt = m.converttofixed(columnFilter, 30);
	    
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
	   // System.out.println("Column filter - line96:" + columnFilt);
	    expr[0].next = null;
	    expr[1] = null;
	    
	  //Perform a filescan using the iterator having a column filter equality condition check
	    FileScan fscan1 = null;
	    FileScan fscan2 = null;
	    
	    try {
	      fscan1 = new FileScan(heapFileName1, attrType, attrSize, (short) 4, 4, projlist, expr);
	      filter_sort(attrType, attrSize,fscan1,"1");
	      //fscan2 = new FileScan(heapFileName2, attrType, attrSize, (short) 4, 4, projlist, expr);
	    }
	    catch (Exception e) {
	      e.printStackTrace();
	    }
	    fscan1.close();
	    
	    
	    try {
		      fscan1 = new FileScan(heapFileName1, attrType, attrSize, (short) 4, 4, projlist, expr);
		      //filter_sort(attrType, attrSize,fscan1,"1");
		      //fscan2 = new FileScan(heapFileName2, attrType, attrSize, (short) 4, 4, projlist, expr);
		    }
		    catch (Exception e) {
		      e.printStackTrace();
		    }
	    
	   

	    removeDup(attrType, attrSize, fscan1,"1");
	    //ScanTest st= new ScanTest ("sortedheapfile1_heap.in") ;
	    
	    
	    try {
	    	fscan2 = new FileScan(heapFileName2, attrType, attrSize, (short) 4, 4, projlist, expr);
	    	filter_sort(attrType, attrSize,fscan2,"2");
	    }
	    catch (Exception e) {
	      e.printStackTrace();
	    }
	    fscan2.close();
	    try {
	    	fscan2 = new FileScan(heapFileName2, attrType, attrSize, (short) 4, 4, projlist, expr);
	    	//filter_sort(attrType, attrSize,fscan2,"2");
	    }
	    catch (Exception e) {
	      e.printStackTrace();
	    }
	    removeDup(attrType, attrSize, fscan2,"2"); 
	    //st= new ScanTest ("sortedheapfile2_heap.in") ;
	    
	    fscan1.close();
	    fscan2.close();
	    
	    RowJoinMain rjm = new RowJoinMain(heapFileName1, heapFileName2, bigtName, columnFilter);
	    
	   
	}
	
	 public void filter_sort(AttrType[] attrType, short[] attrSize, FileScan f,String i)
	  {
	    Tuple t = null;
		t = new Tuple();
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
		 Heapfile hf3 =null;
	    
	    try {
	        hf3 = new Heapfile("filteredheapfile"+i+"_heap.in");
	      }
	    catch(Exception e)
	    {
	    	e.printStackTrace();
	    }
        
	    try {
	        t = f.get_next();
	       
	        }
	        catch(Exception e) {
	        	e.printStackTrace();
	        }
	  
   
       while (t != null) {
       	try {
       		Map map = new Map();
       		
       		
       		//Map to Tuple implementation
       		try {
       			map.setRowLabel(map.converttofixed(t.getStrFld(1),30));
       			map.setColumnLabel(map.converttofixed( t.getStrFld(2),30));
       			map.setValue(map.converttofixed(t.getStrFld(3),50));
       			map.setTimeStamp(t.getIntFld(4));
       			byte[] rec = map.copyMapToTuple(map);
       	   
       	    	//System.out.println("Row Join Line 125"+rec);
       	    	try {
       				hf3.insertMap(rec);
       			} catch (Exception e)
       	    	{
       				e.printStackTrace();
       			}
       		} catch (Exception e1) {
       			e1.printStackTrace();
       		}
       		
       	    
       	  t = f.get_next();
       	}
       	catch(Exception e) {
	        	e.printStackTrace();
	        }
       	
       }
	  }
       
	
	
	
}
