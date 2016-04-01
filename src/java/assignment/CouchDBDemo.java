package assignment;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import com.fourspaces.couchdb.Database;
import com.fourspaces.couchdb.Document;
import com.fourspaces.couchdb.Session;

public class CouchDBDemo {
	public static void main(String[] st) throws Exception
	{
		String dbName ="test_db";
		Session dbSession = new Session("127.0.0.1", 5984);
		Database db = dbSession.getDatabase(dbName);
		
		File file = new File("F:\\allCountries.txt");
//		File file = new File("F:\\file.txt");
		FileInputStream fis= new FileInputStream(file);
		BufferedReader br= new BufferedReader(new InputStreamReader(fis));
		
		String strLine;
		
        while ((strLine = br.readLine()) != null)  
        {
        	try{
        	String[] tokens = strLine.split("\t");
        	int accuracy = 0 ;
        	float latitude = 0;
			float longitude = 0;
			
			
			if(tokens.length>9)
			{
				latitude = Float.parseFloat(tokens[9]);
    			longitude = Float.parseFloat(tokens[10]);
			}
        	if(tokens.length>11 )
        	{
        		accuracy = Integer.parseInt(tokens[11]);
        	}
        	db.saveDocument(getDocument(tokens[0],
        			tokens[1],
        			tokens[2],
        			tokens[3],
        			tokens[4],
        			tokens[5],
        			tokens[6],
        			tokens[7],
        			tokens[8],
        			latitude, 
        			longitude,
        			accuracy));
        	}
        	catch(ArrayIndexOutOfBoundsException e)
    		{
    			e.printStackTrace();
    		}
        }
	}
	
	public static Document getDocument(String countryCode,
			String postalCode,
			String placeName,
			String adminName1,
			String adminCode1,
			String adminName2,
			String adminCode2,
			String adminName3,
			String adminCode3,
			float latitude,
			float longitude,
			int accuracy)
	{
		Document doc = new Document();
		doc.put("countryCode", countryCode);
		doc.put("postalCode", postalCode);
		doc.put("placeName", placeName);
		doc.put("adminName1", adminName1);
		doc.put("adminCode1", adminCode1);
		doc.put("adminName2", adminName2);
		doc.put("adminCode2", adminCode2);
		doc.put("adminName3", adminName3);
		doc.put("adminCode3", adminCode3);
		doc.put("latitude", latitude);
		doc.put("longitude", longitude);
		doc.put("accuracy", accuracy);
		
		return doc;
	}

}
