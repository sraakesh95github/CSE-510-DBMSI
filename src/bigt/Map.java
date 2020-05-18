
package bigt;

import java.lang.*;
import java.util.Arrays;
import global.*;
import java.nio.ByteBuffer;
import heap.Tuple;


public class Map implements GlobalConst{

 /** 
  * Map has fixed structure - RowLabel, ColumnLabel, TimeStamp, Value
  */


  private String RowLabel; 
  private String ColumnLabel;
  private Integer TimeStamp;
  private String Value;

  public Map(){
      RowLabel = new String();
      ColumnLabel = new String();
      TimeStamp = new Integer(0);
      Value = new String();
  }


  /* [TODO]: batchinsert should convert each line in csv file
            to fixed size byte array - size as defined by max range here
  */
  public Map(byte[] amap, int offset){
	  
	  String data= new String(amap);
	  System.out.println(this.RowLabel);
	  String row = data.substring(offset,offset+MAPSTRINGIDX); 
	  this.RowLabel=row.substring(Integer.parseInt(row.substring(0,2))+2,MAPSTRINGIDX);
	  System.out.println(this.RowLabel);
;	  String column = data.substring(offset+MAPSTRINGIDX, offset +2*MAPSTRINGIDX);
	  String val = data.substring(offset + 2*MAPSTRINGIDX, (offset + 2*MAPSTRINGIDX) + MAXMAPVALUE);
	  System.out.println((offset + 2*MAPSTRINGIDX) + MAXMAPVALUE);
	  System.out.println((offset + 2*MAPSTRINGIDX) + MAXMAPVALUE+16);
	  String time =data.substring((offset + 2*MAPSTRINGIDX) + MAXMAPVALUE, (offset + 2*MAPSTRINGIDX) + MAXMAPVALUE+16);
	  System.out.println(time);
	 
	  this.ColumnLabel=column.substring(Integer.parseInt(column.substring(0,2))+2,MAPSTRINGIDX);
	  this.Value = val.substring(Integer.parseInt(val.substring(0,2))+2,MAXMAPVALUE);
	  this.TimeStamp = Integer.parseInt(time.substring(Integer.parseInt(time.substring(0,2))+2,16));
	  
  }

  public Map(Map srcMap){
    this.RowLabel = srcMap.RowLabel;
    this.ColumnLabel = srcMap.ColumnLabel;
    this.Value = srcMap.Value;
    this.TimeStamp = srcMap.TimeStamp;
  }

  public String getRowLabel(){
    return this.RowLabel;
  }

  public String getColumnLabel(){
    return this.ColumnLabel;
  }

  public String getValue(){
    return this.Value;
  }

  public Integer getTimeStamp(){
    return this.TimeStamp;
  }
  
  public void setRowLabel(String val){
    this.RowLabel = val;
  }

  public void setColumnLabel(String val){
    this.ColumnLabel = val;
  }

  public void setTimeStamp(Integer val){
    this.TimeStamp = val;
  }

  public void setValue(String val){
    this.Value = val;
  }
  
  
  public byte[] getMapByteArray(){
  
//    String result=converttofixed(this.RowLabel,30)+
//			converttofixed(this.ColumnLabel,30)+
//			converttofixed(this.Value,50)+
//			converttofixed(Integer.toString(this.TimeStamp),16);
	  
   
//    return result.getBytes();
	  
	  return this.copyMapToTuple(this);
  }
  
  public static String converttofixed(String data,int pad)
	{

		int padding=pad-data.length()-2;
		String res = "";
		if(padding>9) {
			res+=Integer.toString((padding/10));
			res+=Integer.toString((padding%10));
		}
		else {
			res+="0";
			res+=Integer.toString(padding);
		}
		for(int i=0;i<padding;i++) {
			res+='$';
		}
		//System.out.println(res+" "+data);
		return res+data;
	}

  public void print(){
    System.out.println("===MAP DETAILS===");
    System.out.println("Row Label: " + this.RowLabel);
    System.out.println("Column Label: " + this.ColumnLabel);
    System.out.println("Time Stamp: " + this.TimeStamp.toString());
    System.out.println("Value: " + this.Value);
    System.out.println("===END===");
  }

  
  public int size(){
    return (int)(this.getMapByteArray().length);
  }

  /* [TODO]: Figure out what to do with copied attrs.
  */

  public void mapCopy(Map fromMap){
    this.RowLabel = fromMap.RowLabel;
    this.ColumnLabel = fromMap.ColumnLabel;
    this.Value = fromMap.Value;
    this.TimeStamp = fromMap.TimeStamp;
  }


  // Check the following with others
  
  //call: Map myMap = Map.mapInit(amap, offset)

  public static Map mapInit(byte[] amap, int offset){
    Map tempMap = new Map();
    tempMap.RowLabel = Arrays.copyOfRange(amap, offset, offset + MAPSTRINGIDX).toString();
    tempMap.ColumnLabel = Arrays.copyOfRange(amap, offset+MAPSTRINGIDX, offset + 2*MAPSTRINGIDX).toString();
    tempMap.Value = Arrays.copyOfRange(amap, offset + 2*MAPSTRINGIDX, (offset + 2*MAPSTRINGIDX) + MAXMAPVALUE).toString();
    tempMap.TimeStamp = ByteBuffer.wrap(Arrays.copyOfRange(amap, (offset + 2*MAPSTRINGIDX) + MAXMAPVALUE, (offset + 2*MAPSTRINGIDX) + MAXMAPVALUE+16)).getInt();
    return tempMap;
  }

  //call: Map myMap = Map.mapSet(amap, offset)

  public static Map mapSet(byte[] fromMap, int offset){
    return Map.mapInit(fromMap, offset);
  }

  public byte[] copyMapToTuple(Map m) {
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
      
      //Init the Map structure with the headers
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
      
      //Set the header for the Tuples
      try {
          t.setHdr((short) 4, attrType, attrSize);
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      
      try {
    	  //System.out.println(m.getRowLabel());
      	t.setStrFld(1, m.getRowLabel());
            }
            catch (Exception e) {
            e.printStackTrace();
            }
      
      try {
    	  //System.out.println(m.getColumnLabel());
      	t.setStrFld(2, m.getColumnLabel());
            }
            catch (Exception e) {
            e.printStackTrace();
            }
      
      try {
      	t.setStrFld(3, m.getValue());
            }
            catch (Exception e) {
            e.printStackTrace();
            }
      
      try {
      	t.setIntFld(4, (int)m.getTimeStamp());
      	//System.out.println("Set TS value - Map.java: " + Integer.toString(m.getTimeStamp()));
            }
            catch (Exception e) {
            e.printStackTrace();
            }
      
      return t.getTupleByteArray();
  }
}