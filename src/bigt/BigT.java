
package bigt;
import btree.BTreeFile;
import btree.StringKey;
import global.*;
import heap.*;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;
import java.lang.String;
import static tests.TestDriver.OK;
import tests.ScanTest;
//Debug



public class BigT implements GlobalConst,Serializable{
	
	RID start_rid = new RID();
	
	public boolean first_exst_insrt = false;
	
    HashSet<String> hsrow = new HashSet<String>();
    HashSet<String> hscolumn = new HashSet<String>();
    private static short REC_LEN1 = 32;
    
    MID mid =null;
    MID deletemid=null;
    Map map1=null;
    
    HashMap<List<String>, List<MID>> data = new HashMap<List<String>, List<MID>>();
    HashMap<List<String>, List<String>> datatimestamp = new HashMap<List<String>, List<String>>();
    HashMap<List<String>, List<String>> datavalues = new HashMap<List<String>, List<String>>();

    int type= -1;
    String name="";
    String onlyname="";
    boolean status = OK;
    Heapfile hf=null;
    BTreeFile btf1 = null;
    BTreeFile btf2 = null;
 
    int counter = 0;
    public void deleteBigt(String name, int type)throws IOException, HFException, HFBufMgrException, HFDiskMgrException, InvalidSlotNumberException, SpaceNotAvailableException, InvalidTupleSizeException 
    {    	
    	try {
    		hf.deleteFile();
    		btf1.destroyFile();
    		if(type==4|| type==5)
    			btf2.destroyFile();
    	}
    	
    	catch(Exception e)
    	{
    		//System.out.println("BTreeFile Created...");
    		//e.printStackTrace();
    		System.exit(1);
    	}
    }

    public BigT(String name, int type) throws IOException, HFException, HFBufMgrException, HFDiskMgrException, InvalidSlotNumberException, SpaceNotAvailableException, InvalidTupleSizeException {
        this.type=1;
        this.onlyname=name;
        //this.name=name+'_'+Integer.toString(type);
      
        //This operation happens once
        String heapfilename = name+"_heap.in";
    	hf = new Heapfile(name+"_heap.in");

    	System.out.println("Big T - line 75 - Heapfile name: " + heapfilename);
    	
    	if(hf.isHeapFileExst()==true)
    	{	
    		System.out.println("bigt line78 - hf is not null");
    		 FileInputStream fileIn = new FileInputStream(name+".txt");
             ObjectInputStream in = new ObjectInputStream(fileIn);
             try {
				data = (HashMap<List<String>, List<MID>>) in.readObject();
			} 
             catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
             in.close();
    	}
       
    	System.out.println("BigT line64: " + onlyname + "_heap.in");
    	System.out.println("Heapfile created..."+hf);
       
       try {
    	   if(type==2 || type==3 )
       		{
    		   btf1 = new BTreeFile(this.name+"_btree.in",AttrType.attrString, REC_LEN1,0);
       		}
    	   else
           {
               btf1 = new BTreeFile(this.name+"_combined_key.in",AttrType.attrString, REC_LEN1,0);
               btf2 = new BTreeFile(this.name+"_time_stamp.in",AttrType.attrString, REC_LEN1,0);
           }
       }
       catch(Exception e)
       {
    	   System.out.println("BTreeFile Created...");
    	   //e.printStackTrace();
    	   System.exit(1);
       }
    }
    
    public static Stream openStream(String filename,int orderType, String rowFilter, String columnFilter, String ValueFilter)  throws Exception, IOException, HFException, HFBufMgrException, HFDiskMgrException, InvalidSlotNumberException, SpaceNotAvailableException, InvalidTupleSizeException 
    {	
    	return new Stream(filename,orderType,rowFilter,columnFilter,ValueFilter);
    }


    public MID insertMap(Map m) throws IOException, HFException, HFBufMgrException, HFDiskMgrException, InvalidSlotNumberException, SpaceNotAvailableException, InvalidTupleSizeException {
    	
        
    	byte[] mapPtr = m.copyMapToTuple(m);
  
    	String rowLabel=m.getRowLabel().substring(2+Integer.parseInt(m.getRowLabel().substring(0,2)));
        String ColumnLabel = m.getColumnLabel().substring(2+Integer.parseInt(m.getColumnLabel().substring(0,2)));
        String Value = m.getValue().substring(2+Integer.parseInt(m.getValue().substring(0,2)));
        String TimeStamp=Integer.toString(m.getTimeStamp());
 
        
        String timestamp_temp =null, value_temp=null;
        List<String> lis=new ArrayList<String>();
        lis.add(rowLabel);
        lis.add(ColumnLabel);
        
        try {
        	if(data.containsKey(lis))
            {
            	if(data.get(lis).size()==3)
            	{
            		deletemid =data.get(lis).get(0);
            		data.get(lis).remove(0);
            		
            		if(type==4 || type==5 )
               		{
            		
	            		timestamp_temp =datatimestamp.get(lis).get(0);
	            		datatimestamp.get(lis).remove(0);
	            		
	            		value_temp =datavalues.get(lis).get(0);
	            		datavalues.get(lis).remove(0);
               		}
            		
            	}
	            	hsrow.add(rowLabel);
	                hscolumn.add(ColumnLabel);
	                if(first_exst_insrt == true)
	                {
	                	first_exst_insrt = false;
	                	String heapName = this.onlyname + "_heap.in";
	                	//ScanTest s = new ScanTest(heapName);
	                	ScanTest s = new ScanTest();
	                	s.MyScanTest(heapName,0);
	                	start_rid = s.getRid();
	                }
	                else
	                {
	                	start_rid = null;
	                }
            		mid = hf.insertMap(mapPtr);
            		data.get(lis).add(mid);
            		if(type==4 || type==5 )
               		{
	            		datatimestamp.get(lis).add(TimeStamp);
	            		datavalues.get(lis).add(Value);
               		}
            }
            else
            {
            	 hsrow.add(rowLabel);
                 hscolumn.add(ColumnLabel);
                 if(first_exst_insrt == true)
	                {
	                	first_exst_insrt = false;
	                	String heapName = this.onlyname + "_heap.in";
	                	//ScanTest s = new ScanTest(heapName);
	                	ScanTest s = new ScanTest();
	                	s.MyScanTest(heapName,0);
	                	start_rid = s.getRid();
	                }
	                else
	                {
	                	start_rid = null;
	                }
                 mid = hf.insertMap(mapPtr);
     		
                 List<MID> mid_list=new ArrayList<MID>();
                 mid_list.add(mid);
                 data.put(lis,mid_list);
                 
                 if(type==4 || type==5 )
            	 {
	                 List<String> timestamp_string=new ArrayList<String>();
	                 timestamp_string.add(TimeStamp);
	                 datatimestamp.put(lis,timestamp_string);
	                 
	                 List<String> datavalues_string=new ArrayList<String>();
	                 datavalues_string.add(Value);
	                 datavalues.put(lis,datavalues_string);
            	 }

            }

        }
        
        catch(Exception e)
        {
        	e.printStackTrace();
        	System.exit(1);
        }
        
        if(type==1)
        {
        	try {
        		if (deletemid!=null) {
        		hf.deleteMap(deletemid);
        		List<String> lbl_lis=new ArrayList<String>();
        		lbl_lis.add("Malawi");
        		lbl_lis.add("Prionace_");
        		//System.out.println("BigT - line227: " + data.get(lbl_lis));
        		deletemid=null;
        		}
        	}
        	
        	catch(Exception e)
        	{
        		e.printStackTrace();
        	}
        }
        
        if (type==2)
        {
        	 try {
             	
             	if (deletemid!=null) {
             		//btf1.Delete(new StringKey(rowLabel), mid.midToRid(deletemid));
             		hf.deleteMap(deletemid);
             		deletemid=null;
             		}
             	btf1.insert(new StringKey(rowLabel), mid.midToRid(mid));
             }
             catch (Exception e) {
                 //e.printStackTrace();
                 System.exit(1);
             }
        	}

        if (type==3) {
          
        	 try {
 	            if(deletemid!=null)
 	            {
 	            	//btf1.Delete(new StringKey(ColumnLabel), mid.midToRid(deletemid));
 	        		hf.deleteMap(deletemid);
 	        		deletemid=null;	
 	            }
 	           btf1.insert(new StringKey(ColumnLabel), mid.midToRid(mid));
             }
             catch(Exception e){
             	//e.printStackTrace();
             	System.exit(1);
             }
        } 
  
               

        if (type==4)
        {
            try {
            	int max=Math.min(30,(ColumnLabel+rowLabel).length());
            	
            	if (deletemid!=null) {
            		//btf2.Delete(new StringKey(timestamp_temp), mid.midToRid(deletemid));
            		//btf1.Delete(new StringKey((ColumnLabel+rowLabel).substring(0,max)), mid.midToRid(deletemid));
            		hf.deleteMap(deletemid);
            		deletemid=null;
            	}
            	
            	
                btf1.insert(new StringKey((ColumnLabel+rowLabel).substring(0,max)), mid.midToRid(mid));
                btf2.insert(new StringKey(TimeStamp), mid.midToRid(mid));

            }
            catch (Exception e) {
               // e.printStackTrace();
                System.exit(1);
            }


        }
        if (type==5)
        {
        	
        	try {
            	if (deletemid!=null) {
            		//btf2.Delete(new StringKey(timestamp_temp), mid.midToRid(deletemid));
            		//btf1.Delete(new StringKey((rowLabel+value_temp).substring(0,Math.min(30,(rowLabel+value_temp).length()))), mid.midToRid(deletemid));
            		
            		hf.deleteMap(deletemid);
            		deletemid=null;
            	}
            	

                btf1.insert(new StringKey((rowLabel+Value).substring(0,Math.min(30,(rowLabel+Value).length()))), mid.midToRid(mid));
                btf2.insert(new StringKey(TimeStamp), mid.midToRid(mid));

            }
            catch (Exception e) {
               // e.printStackTrace();
                System.exit(1);
            }
        }
        return mid;
    }

    public int getRowCnt(){

        return hsrow.size();
    }
    
    public int getRecordCount() {
    	int count = 0;
    	try {
    	count = hf.getRecCnt();
    }
    	catch(Exception e) {
    		e.printStackTrace();
    	}
    	return count;
    }
    
    public int getColumnCnt(){

        return hscolumn.size();
    }
    
    public Heapfile getHeapFile() {
    	return hf;
    }
    
    public String getHeapFileName() {
    	return this.onlyname+"_heap.in";
    }

	public void closeBTree() {
		if(btf1!=null) {
			try {
		this.btf1.close();
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		if(btf2!=null) {
			try {
				this.btf2.close();
					}
					catch(Exception e) {
						e.printStackTrace();
					}
		}
	}
	
	public HashMap<List<String>, List<MID>>  getHashMap(){
    	return data;
    }
    
}