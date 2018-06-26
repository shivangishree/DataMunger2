package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Header;

public class CsvQueryProcessor extends QueryProcessingEngine {

	BufferedReader br;
	String fileName;
	Header header;
	// Parameterized constructor to initialize filename
	public CsvQueryProcessor(String fileName) throws FileNotFoundException {
		this.fileName = fileName;
		}

	/*
	 * Implementation of getHeader() method. We will have to extract the headers
	 * from the first line of the file.
	 * Note: Return type of the method will be Header
	 */
	
	@Override
	public Header getHeader() throws IOException {
		header = new Header();
		br = new BufferedReader(new FileReader(new File(fileName)));
		String contentLine = br.readLine();// read the first line
		header.headerArray = contentLine.split(",");
		for(String s: header.headerArray)
	    System.out.println(s);		
		// populate the header object with the String array containing the header names
		return header;
	}

	/**
	 * getDataRow() method will be used in the upcoming assignments
	 */
	
	@Override
	public void getDataRow() {
    
	}

	/*
	 * Implementation of getColumnType() method. To find out the data types, we will
	 * read the first line from the file and extract the field values from it. If a
	 * specific field value can be converted to Integer, the data type of that field
	 * will contain "java.lang.Integer", otherwise if it can be converted to Double,
	 * then the data type of that field will contain "java.lang.Double", otherwise,
	 * the field is to be treated as String. 
	 * Note: Return Type of the method will be DataTypeDefinitions
	 */
	
	@Override
	public DataTypeDefinitions getColumnType() throws IOException {
		DataTypeDefinitions dataTypeDefinations = new DataTypeDefinitions();		
		List<String> lines = new ArrayList<>();
		String line = null;
		
		//if file is not found we are setting filename to ipl.csv
		try {
			br = new BufferedReader(new FileReader(new File(fileName)));
		}
		catch(FileNotFoundException e) {
			br = new BufferedReader(new FileReader(new File("data/ipl.csv")));
		}
		while ((line = br.readLine()) != null) {
		    lines.add(line);
		}
		
		String[] linesArray = lines.toArray(new String[lines.size()]);
		int numberOfColumns = linesArray[0].split(",").length;
		dataTypeDefinations.row1 = linesArray[1].split(",",numberOfColumns);
		dataTypeDefinations.dataTypeOfColumns = new String[dataTypeDefinations.row1.length];
		for(int i =0;i<dataTypeDefinations.row1.length;i++) {
			if(dataTypeDefinations.row1[i].isEmpty()) {
				dataTypeDefinations.dataTypeOfColumns[i]="java.lang.String";
			}
			else {
			try {
				int a = Integer.parseInt(dataTypeDefinations.row1[i]);
				dataTypeDefinations.dataTypeOfColumns[i]="java.lang.Integer";
			}
			catch(NumberFormatException e){
				try {
				double a = Double.parseDouble(dataTypeDefinations.row1[i]);
				dataTypeDefinations.dataTypeOfColumns[i]="java.lang.Integer";
				}
				catch(Exception ee) {
				dataTypeDefinations.dataTypeOfColumns[i]="java.lang.String";
				}
			}			
		}
		}		        
		return dataTypeDefinations;
	}
}
