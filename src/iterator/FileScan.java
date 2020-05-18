package iterator;
   

import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;


import java.lang.*;
import java.io.*;

/**
 *open a heapfile and according to the condition expression to get
 *output file, call get_next to get all tuples
 */
public class FileScan extends  Iterator
{
  private AttrType[] _in1;
  private short in1_len;
  private short[] s_sizes; 
  private Heapfile f;
  private Scan scan;
  private Tuple     tuple1;
  private Tuple    Jtuple;
  private int        t1_size;
  private int nOutFlds;
  private CondExpr[]  OutputFilter;
  public FldSpec[] perm_mat;
  
  boolean debug = false;

 

  /**
   *constructor
   *@param file_name heapfile to be opened
   *@param in1[]  array showing what the attributes of the input fields are. 
   *@param s1_sizes[]  shows the length of the string fields.
   *@param len_in1  number of attributes in the input tuple
   *@param n_out_flds  number of fields in the out tuple
   *@param proj_list  shows what input fields go where in the output tuple
   *@param outFilter  select expressions
   *@exception IOException some I/O fault
   *@exception FileScanException exception from this class
   *@exception TupleUtilsException exception from this class
   *@exception InvalidRelation invalid relation 
   */
  public  FileScan (String  file_name,
		    AttrType in1[],                
		    short s1_sizes[], 
		    short     len_in1,              
		    int n_out_flds,
		    FldSpec[] proj_list,
		    CondExpr[] outFilter        		    
		    )
    throws IOException,
	   FileScanException,
	   TupleUtilsException, 
	   InvalidRelation
    {
      _in1 = in1; 
      in1_len = len_in1;
      s_sizes = s1_sizes;
      
      Jtuple =  new Tuple();
      AttrType[] Jtypes = new AttrType[n_out_flds];
      short[]    ts_size;
      ts_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes, in1, len_in1, s1_sizes, proj_list, n_out_flds);
      
      OutputFilter = outFilter;
      perm_mat = proj_list;
      nOutFlds = n_out_flds; 
      tuple1 =  new Tuple();

      try {
	tuple1.setHdr(in1_len, _in1, s1_sizes);
      }catch (Exception e){
	throw new FileScanException(e, "setHdr() failed");
      }
      t1_size = tuple1.size();
      
      try {
	f = new Heapfile(file_name);
	
      }
      catch(Exception e) {
	throw new FileScanException(e, "Create new heapfile failed");
      }
      
      try {
	scan = f.openScan();
      }
      catch(Exception e){
	throw new FileScanException(e, "openScan() failed");
      }
    }
  
  /**
   *@return shows what input fields go where in the output tuple
   */
  public FldSpec[] show()
    {
      return perm_mat;
    }
  
  /**
   *@return the result tuple
   *@exception JoinsException some join exception
   *@exception IOException I/O errors
   *@exception InvalidTupleSizeException invalid tuple size
   *@exception InvalidTypeException tuple type not valid
   *@exception PageNotReadException exception from lower layer
   *@exception PredEvalException exception from PredEval class
   *@exception UnknowAttrType attribute type unknown
   *@exception FieldNumberOutOfBoundException array out of bounds
   *@exception WrongPermat exception for wrong FldSpec argument
   */
  public Tuple get_next()
    throws JoinsException,
	   IOException,
	   InvalidTupleSizeException,
	   InvalidTypeException,
	   PageNotReadException, 
	   PredEvalException,
	   UnknowAttrType,
	   FieldNumberOutOfBoundException,
	   WrongPermat
    {     
      RID rid = new RID();;
      
      while(true) {
	if((tuple1 =  scan.getNext(rid)) == null) {
	  return null;
	}
	
	tuple1.setHdr(in1_len, _in1, s_sizes);
	if (PredEval.Eval(OutputFilter, tuple1, null, _in1, null) == true){
	  Projection.Project(tuple1, _in1,  Jtuple, perm_mat, nOutFlds); 
	  //System.out.println("FileScan - line138: " + Jtuple.getStrFld(1));
	  String[] mapFields = new String[3]; 
	  mapFields = unpadAttributes(Jtuple.getStrFld(1), Jtuple.getStrFld(2), Jtuple.getStrFld(3));
	  Jtuple.setStrFld(1, mapFields[0]);
	  Jtuple.setStrFld(2, mapFields[1]);
	  Jtuple.setStrFld(3, mapFields[2]);
	  Jtuple.setIntFld(4, Jtuple.getIntFld(4));
	  if(debug)
	  {
	  System.out.println("FileScan - line138: " + Jtuple.getStrFld(1));
	  }
	  return  Jtuple;
	}        
      }
    }
  
  /**
   *implement the abstract method close() from super class Iterator
   *to finish cleaning up
   */
  public void close() 
    {
     
      if (!closeFlag) {
	scan.closescan();
	closeFlag = true;
      } 
    }
  
    //File to unpad the saved attributes
	public String[] unpadAttributes(String RL, String CL, String Value) {
		String[]ToUnPad= new String[3];
		ToUnPad[0] = RL.substring(2+Integer.parseInt(RL.substring(0,2)));
		ToUnPad[1] = CL.substring(2+Integer.parseInt(CL.substring(0,2)));
		ToUnPad[2] = Value.substring(2+Integer.parseInt(Value.substring(0,2)));
		return ToUnPad;
	}
  
}


