package tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import bigt.BigT;
import bigt.Map;
import global.*;
import heap.FieldNumberOutOfBoundException;
import heap.Heapfile;
import heap.Tuple;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;

public class RowJoinMain implements GlobalConst {
	
	public String findThird(String rowFilter, int timestamp, int i) {
		 
		FileScan fscan = null;
		 
		 bigt.Map m = new bigt.Map();
		 
		 AttrType[] attrType = new AttrType[4];
			attrType[0] = new AttrType(AttrType.attrString);
			attrType[1] = new AttrType(AttrType.attrString);
			attrType[2] = new AttrType(AttrType.attrString);
			attrType[3] = new AttrType(AttrType.attrInteger);
			
			short[] attrSize = new short[4];
			attrSize[0] = 30;
			attrSize[1] = 30;
			attrSize[2] = 50;
			
			String rowFilt = m.converttofixed(rowFilter, 30);
			
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
		    expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
		    expr[0].operand2.string = rowFilt;
		    expr[0].next = null;
		    expr[1] = null;
		    
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
			int resTime = 0;
			String resValue = "";
			
		 try {
				
		      fscan = new FileScan("filteredheapfile"+i+"_heap.in", attrType, attrSize, (short) 4, 4, projlist, expr);
		      		
	}
		 catch (Exception e) {
		      e.printStackTrace();
		    }
 
		 //get the values from file scan
		    try {
		        t = fscan.get_next();
		       
		        }
		        catch(Exception e) {
		        	e.printStackTrace();
		        }
		    
		    try {
				while (t != null && t.getIntFld(4)!=timestamp) {
					
					try {
						resTime = t.getIntFld(4);
						resValue = t.getStrFld(3);
						t = fscan.get_next();
						}
						catch (Exception e) {
							e.printStackTrace();
						}
					}
			} catch (FieldNumberOutOfBoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		   // System.out.println(resTime);
		 return resValue+" "+resTime;
	}
	
	
public RowJoinMain(String heapFileName1, String heapFileName2, String bigtName, String columnFilter) {
	
	BigT bigt = null;
	try{
		bigt = new BigT(bigtName, 1);
	}
	catch(Exception e) {
		e.printStackTrace();
	}
	MID mid=null;
	Map map;
	
	
	
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
	    //System.out.println("Column filter - line96:" + columnFilt);
	    expr[0].next = null;
	    expr[1] = null;
		
		
	//Perform a filescan using the iterator having a column filter equality condition check
    FileScan fscan1 = null;
    FileScan fscan2 = null;
    
    try {
      fscan1 = new FileScan("sortedheapfile1_heap.in", attrType, attrSize, (short) 4, 4, projlist, expr);
      fscan2 = new FileScan("sortedheapfile2_heap.in", attrType, attrSize, (short) 4, 4, projlist, expr);
    }
    catch (Exception e) {
      e.printStackTrace();
    }

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
	
	//Setup the tuple to accept the values
		Tuple t1 = null;
		t1 = new Tuple();
		try {
		    t1.setHdr((short) 4, attrType, attrSize);
		  }
		  catch (Exception e) {
		    e.printStackTrace();
		  }
		
		//Get the size of the tuple
		int size1 = t1.size();
		//System.out.println(Integer.toString(size));
		
		//Create a Tuple with the pre-defined size
		t1 = new Tuple(size1);
		
		//Init the Map structure with the headers
		try {
		    t1.setHdr((short) 4, attrType, attrSize);
		  }
		  catch (Exception e) {
		    e.printStackTrace();
		  }
   
    //get the values from file scan
    try {
        t = fscan1.get_next();
       
        }
        catch(Exception e) {
        	e.printStackTrace();
        }
        
    HashMap<String, List<String>> data = new HashMap<String, List<String>>();
      
    //Scan through the sorted stream
    	while (t != null) {
    		
    		List<String> valuelist=new ArrayList<String>();
    	    try {
    	    	fscan2 = new FileScan("sortedheapfile2_heap.in", attrType, attrSize, (short) 4, 4, projlist, expr);
    	        t1 = fscan2.get_next();
    	        //System.out.println("RowJoinMain - Line 105 - "+t.getStrFld(1)+t.getStrFld(2)+t.getStrFld(3)+t.getIntFld(4));
    	        //Scan through the sorted stream
    	        while (t1 != null) {
    	        	
    	        	try {
    	        		if(t.getStrFld(3).equals(t1.getStrFld(3))) {
    	        			
    	        			valuelist.add(t1.getStrFld(1));
    	        		//System.out.println("RowJoinMain - Line 252 - "+t.getStrFld(1)+":"+t1.getStrFld(1)+" "+t.getStrFld(2)+" "+t.getStrFld(3)+" "+t.getIntFld(4));
    	        		map = new Map();
    					//Map to Tuple implementation
    					map.setRowLabel(map.converttofixed(t.getStrFld(1)+":"+t1.getStrFld(1),30));
    					map.setColumnLabel(map.converttofixed(t.getStrFld(2),30));
    					map.setValue(map.converttofixed(t.getStrFld(3),50));
    					map.setTimeStamp(t.getIntFld(4));

    					try {
    						mid = bigt.insertMap(map);
    					}
    						
    					catch(Exception e)
    					{
    						e.printStackTrace();
    					}
    					
    	        		//System.out.println("RowJoinMain - Line 253 - "+t.getStrFld(1)+":"+t1.getStrFld(1)+" "+t1.getStrFld(2)+" "+t1.getStrFld(3)+" "+t1.getIntFld(4));
    	        		map = new Map();
    					//Map to Tuple implementation
    					map.setRowLabel(map.converttofixed(t.getStrFld(1)+":"+t1.getStrFld(1),30));
    					map.setColumnLabel(map.converttofixed(t1.getStrFld(2),30));
    					map.setValue(map.converttofixed(t1.getStrFld(3),50));
    					map.setTimeStamp(t1.getIntFld(4));

    					try {
    						mid = bigt.insertMap(map);
    					}
    						
    					catch(Exception e)
    					{
    						e.printStackTrace();
    					}
    	        		String leftTable = findThird(t.getStrFld(1),t.getIntFld(4),1);
    	        		String rightTable = findThird(t1.getStrFld(1),t1.getIntFld(4),2);
    	        		if(Integer.parseInt(leftTable.split(" ")[1])>0 && Integer.parseInt(rightTable.split(" ")[1])>0)
    	        		{
	    	        		if(Integer.parseInt(leftTable.split(" ")[1])>Integer.parseInt(rightTable.split(" ")[1]))
		    	        		{
		        	        		//System.out.println("RowJoinMain - Line 258 - "+t.getStrFld(1)+":"+t1.getStrFld(1)+" "+t.getStrFld(2)+" "+leftTable.split(" ")[0]+" "+leftTable.split(" ")[1]);
		        	        		map = new Map();
		        					//Map to Tuple implementation
		        					map.setRowLabel(map.converttofixed(t.getStrFld(1)+":"+t1.getStrFld(1),30));
		        					map.setColumnLabel(map.converttofixed(t.getStrFld(2),30));
		        					map.setValue(map.converttofixed(leftTable.split(" ")[0],50));
		        					map.setTimeStamp(Integer.parseInt(leftTable.split(" ")[1]));
		
		        					try {
		        						mid = bigt.insertMap(map);
		        					}
		        						
		        					catch(Exception e)
		        					{
		        						e.printStackTrace();
		        					}
		    	        		}
		    	        		else {
		        	        		//System.out.println("RowJoinMain - Line 261 - "+t.getStrFld(1)+":"+t1.getStrFld(1)+" "+t.getStrFld(2)+" "+rightTable.split(" ")[0]+" "+rightTable.split(" ")[1]);
		        	        		map = new Map();
		        					//Map to Tuple implementation
		        					map.setRowLabel(map.converttofixed(t.getStrFld(1)+":"+t1.getStrFld(1),30));
		        					map.setColumnLabel(map.converttofixed(t.getStrFld(2),30));
		        					map.setValue(map.converttofixed(rightTable.split(" ")[0],50));
		        					map.setTimeStamp(Integer.parseInt(rightTable.split(" ")[1]));
		
		        					try {
		        						mid = bigt.insertMap(map);
		        					}
		        						
		        					catch(Exception e)
		        					{
		        						e.printStackTrace();
		        					}
		    	        		}
	    	        		}
    
   	        		}
    	        		t1 = fscan2.get_next();
    	        		}
    	        		catch (Exception e) {
    	        			e.printStackTrace();
    	        		}
    	        	}
    	        if(!valuelist.isEmpty())
    	        	data.put(t.getStrFld(1),valuelist);
    	        	
    	        	
    	        	t = fscan1.get_next();
    		
    	    	}
    	    	catch (Exception e) {
    	    		e.printStackTrace();
    	    	}
    	  }
    	
 
    	for (String name : data.keySet())  
    	{
    		List<String> values=data.get(name);
    		for(String value : values)
    		{
    		
    			 FileScan fscan3 = null;
    			    FileScan fscan4 = null;
    			    m = new bigt.Map();
    			    try {
    			    			
    					String namefilter = m.converttofixed(name, 30);
    			    	expr = new CondExpr[2];
    				    expr[0] = new CondExpr();
    				    expr[0].op = new AttrOperator(AttrOperator.aopEQ);
    				    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    				    expr[0].type2 = new AttrType(AttrType.attrString);
    				    expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
    				    expr[0].operand2.string = namefilter;
    				    expr[0].next = null;
    				    expr[1] = null;
    					
    			      fscan3 = new FileScan(heapFileName1, attrType, attrSize, (short) 4, 4, projlist, expr);
    			      
    			      String valuefilter = m.converttofixed(value, 30);
    			      expr = new CondExpr[2];
	  				    expr[0] = new CondExpr();
	  				    expr[0].op = new AttrOperator(AttrOperator.aopEQ);
	  				    expr[0].type1 = new AttrType(AttrType.attrSymbol);
	  				    expr[0].type2 = new AttrType(AttrType.attrString);
	  				    expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
	  				    expr[0].operand2.string = valuefilter;
	  				    expr[0].next = null;
	  				    expr[1] = null;
	    			      fscan4 = new FileScan(heapFileName2, attrType, attrSize, (short) 4, 4, projlist, expr);
    			   
		    }
    			    catch (Exception e) {
    			      e.printStackTrace();
    			    }
    			    
    			    Tuple ta = null;
    				ta = new Tuple();
    				try {
    				    ta.setHdr((short) 4, attrType, attrSize);
    				  }
    				  catch (Exception e) {
    				    e.printStackTrace();
    				  }
    				
    				//Get the size of the tuple
    				int sizea = ta.size();
    				//System.out.println(Integer.toString(size));
    				
    				//Create a Tuple with the pre-defined size
    				ta = new Tuple(sizea);
    				
    				//Init the Map structure with the headers
    				try {
    				    ta.setHdr((short) 4, attrType, attrSize);
    				  }
    				  catch (Exception e) {
    				    e.printStackTrace();
    				  }
    				
    				//Setup the tuple to accept the values
    					Tuple tb = null;
    					tb = new Tuple();
    					try {
    					    tb.setHdr((short) 4, attrType, attrSize);
    					  }
    					  catch (Exception e) {
    					    e.printStackTrace();
    					  }
    					
    					//Get the size of the tuple
    					int sizeb = tb.size();
    					//System.out.println(Integer.toString(size));
    					
    					//Create a Tuple with the pre-defined size
    					tb = new Tuple(sizeb);
    					
    					//Init the Map structure with the headers
    					try {
    					    tb.setHdr((short) 4, attrType, attrSize);
    					  }
    					  catch (Exception e) {
    					    e.printStackTrace();
    					  }
    			  			
    			    //get the values from file scan
    			    try {
    			        ta = fscan3.get_next();
    			       
    			        }
    			        catch(Exception e) {
    			        	e.printStackTrace();
    			        }
    			    
    			    while (ta != null) {
        	        	
        	        	try {
        	        	
        	        		if(!ta.getStrFld(2).equals(columnFilter)) {
        	        			map = new Map();
            					//Map to Tuple implementation
            					map.setRowLabel(map.converttofixed(ta.getStrFld(1)+":"+value,30));
            					map.setColumnLabel(map.converttofixed(ta.getStrFld(2)+"_left",30));
            					map.setValue(map.converttofixed(ta.getStrFld(3),50));
            					map.setTimeStamp(ta.getIntFld(4));

            					try {
            						mid = bigt.insertMap(map);
            					}
            						
            					catch(Exception e)
            					{
            						e.printStackTrace();
            					}
        	        		}
        	        		//System.out.println("RowJoinMain - Line 285 - "+ta.getStrFld(1)+":"+value+" "+ta.getStrFld(2)+"_left "+ta.getStrFld(3)+" "+ta.getIntFld(4));    		
        	        		
        	        		ta = fscan3.get_next();
        	        		}
        	        		catch (Exception e) {
        	        			e.printStackTrace();
        	        		}
        	        	}
    			    try {
    			        tb = fscan4.get_next();
    			       
    			        }
    			        catch(Exception e) {
    			        	e.printStackTrace();
    			        }
    			        
    			    
    			    while (tb != null) {
        	        	
        	        	try {
        	        	
        	        		if(!tb.getStrFld(2).equals(columnFilter))
        	        		{
        	        			map = new Map();
            					//Map to Tuple implementation
            					map.setRowLabel(map.converttofixed(name+":"+tb.getStrFld(1),30));
            					map.setColumnLabel(map.converttofixed(tb.getStrFld(2)+"_right",30));
            					map.setValue(map.converttofixed(tb.getStrFld(3),50));
            					map.setTimeStamp(tb.getIntFld(4));

            					try {
            						mid = bigt.insertMap(map);
            					}
            						
            					catch(Exception e)
            					{
            						e.printStackTrace();
            					}
        	        		}
        	        		//System.out.println("RowJoinMain - Line 308 - "+name+":"+tb.getStrFld(1)+" "+tb.getStrFld(2)+"_right "+tb.getStrFld(3)+" "+tb.getIntFld(4));
        	        		
        	        		tb = fscan4.get_next();
        	        		}
        	        		catch (Exception e) {
        	        			e.printStackTrace();
        	        		}
        	        	}
    			    
    		}
    	}
    	try {
			ScanTest st= new ScanTest () ;
			st.MyScanTest(bigtName,1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
}

}
