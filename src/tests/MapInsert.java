package tests;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Scanner;

import bigt.BigT;
import bigt.Map;
import diskmgr.PCounter;
import global.GlobalConst;
import global.MID;
import global.SystemDefs;
import heap.Heapfile;

public class MapInsert extends TestDriver
implements GlobalConst{

public BigT mapinsert(String Rowlabel, String Columnlabel, String Val, String TS, int bigtType, String bigtName){
		
		TestDriver tBatchInsert = new TestDriver ("phase");
		
		
		BigT bigt = null;
		try{
			bigt = new BigT(bigtName, bigtType);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	
		
		MID mid=null;

			
		Map map = new Map();
		
		//Map to Tuple implementation
		map.setRowLabel(map.converttofixed(Rowlabel,30));
		map.setColumnLabel(map.converttofixed(Columnlabel,30));
		map.setValue(map.converttofixed(Val,50));
		map.setTimeStamp(Integer.parseInt(TS));


	
		try {
			mid = bigt.insertMap(map);
		}
			
		catch(Exception e)
		{
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
		
		//System.out.println("Rcounter"+Integer.toString(PCounter.rcounter));
		//System.out.println("Wcounter"+Integer.toString(PCounter.wcounter));
		System.out.println("\n<" + Rowlabel + " - " + Columnlabel + " - " + Val + " - " + TS +">\n");
		return bigt;

		
	}
}

