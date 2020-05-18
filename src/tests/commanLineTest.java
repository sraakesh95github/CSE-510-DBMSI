package tests;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Scanner;
//import tests.RowJoin;

import diskmgr.PCounter;
import diskmgr.bigDB;

import bigt.BigT;
import global.*;
import iterator.Sort;

	class CommanLineTest extends TestDriver
	implements GlobalConst,Serializable{
	static String file_storage="bigtfile.txt";
	public static void main(String[] args) throws FileNotFoundException{
		//TestDriver tBatchInsert = new TestDriver ("BatchTest");
		String dbpath = "phase2-group4.minibase-db";
		String filepath = "C:\\SR files\\College\\Subjects\\DBMSI\\Project\\Phase 3\\Demo version\\Group 4\\Minibase\\";
		String filepath_user2 = "/Phase-3/Minibase/Minibase - Copy/src/";
		
		if(args[0].equals("batchinsert"))
		{
			
			//Check for the existing DB
			File tmpDir = new File(filepath + dbpath);			
			boolean fileExists = tmpDir.exists();

			SystemDefs sysdef = null;
			
			if(!fileExists) {
			sysdef = new SystemDefs(dbpath, 50000, Integer.parseInt(args[4]), "Clock");
			}
			else
			{
			sysdef = new SystemDefs(dbpath, 0, Integer.parseInt(args[4]), "Clock" );
			}
			try {
				BigT bigt = null;
				BatchInsert b=new BatchInsert();
				bigt=b.batchinsert(args[1], Integer.parseInt(args[2]), args[3]);
				//ScanTest s = new ScanTest();
				sysdef.closebigDB();
				
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			
		}
		else if(args[0].equals("mapinsert"))
		{
			
			//Check for the existing DB
			File tmpDir = new File(filepath + dbpath);			
			boolean fileExists = tmpDir.exists();

			SystemDefs sysdef = null;
			if(!fileExists) {
			sysdef = new SystemDefs(dbpath, 36000, Integer.parseInt(args[4]), "Clock");
			}
			else
			{
			sysdef = new SystemDefs(dbpath, 0, Integer.parseInt(args[4]), "Clock" );
			}
			try {
				BigT bigt = null;
				MapInsert mi=new MapInsert();
				bigt=mi.mapinsert(args[1], args[2],args[3],args[4],Integer.parseInt(args[5]),args[6]);
				//ScanTest s = new ScanTest();
				sysdef.closebigDB();
				
			}
			
			catch(Exception e)
			{
				e.printStackTrace();
			}
			System.out.println("Commandline - line86: mapinsert");
			
		}
		else if (args[0].equals("query"))
		{
			//System.out.println(Integer.parseInt(args[5]));
			SystemDefs sysdef = new SystemDefs(dbpath, 0,Integer.parseInt(args[7]), "Clock" );
			BigT bigt = null;
			//BatchInsert b=new BatchInsert();
			//bigt=b.batchinsert("D:/project2_testdata.csv",2,"bigtable");
			
			try {
				QueryInterface q = new 	QueryInterface(args[1],Integer.parseInt(args[2]),Integer.parseInt(args[3]),args[4],args[5],args[6]);
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			
			System.out.println("Rcounter"+Integer.toString(PCounter.rcounter));
			System.out.println("Wcounter"+Integer.toString(PCounter.wcounter));
			
		}

		
		else if(args[0].equals("rowsort"))
		{
			System.out.println("Main - line84 :" + args[1] + " " + args[2]);
			SystemDefs sysdef = new SystemDefs(dbpath, 0, Integer.parseInt(args[4]), "Clock" );
			String heapFileName = args[1];
			RowSort2 r = new RowSort2(heapFileName, args[2], args[3]);
		}
		else if (args[0].equals("scan"))
		{
			SystemDefs sysdef = new SystemDefs(dbpath, 0, Integer.parseInt(args[2]), "Clock" );
			System.out.println("CL line78:" + args[1]);
			try {
				ScanTest s = new ScanTest();
				s.MyScanTest(args[1], 1);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		else if(args[0].equals("rowjoin"))
		{
//			System.out.println("Main - line84 :" + args[1] + " " + args[2]);
			SystemDefs sysdef = new SystemDefs(dbpath, 0, Integer.parseInt(args[5]), "Clock" );
			RowJoin r = new RowJoin(args[1], args[2], args[3], args[4]);
		}
		else if(args[0].equals("getCounts"))
		{
			SystemDefs sysdef = new SystemDefs(dbpath, 0, Integer.parseInt(args[2]), "Clock" );
			ScanTest st = new ScanTest();
			try
			{
			System.out.println(st.MyScanTest(args[1], 0));
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			System.out.println("Rcounter:"+Integer.toString(PCounter.rcounter));
			System.out.println("Wcounter:"+Integer.toString(PCounter.wcounter));
		}
			
	}
	

}