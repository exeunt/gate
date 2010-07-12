
import gate.Factory;
import gate.Gate;
import gate.Document;
import gate.persist.PersistenceException;
import gate.persist.SerialDataStore;
import gate.util.Err;
import gate.util.GateException;

import java.io.File;
import java.net.URL;


public class DataStoreApp {
	private static final String DS_DIR = "C:\\Documents and Settings\\slee\\Desktop\\Filing Data Store";
	private static final String DOC_DIR = "C:\\Documents and Settings\\slee\\Desktop\\raw data sample"; 

	public DataStoreApp() {
	}

	public static void main(String[] args) {
		try {
			Gate.init();
		}
		catch (GateException gex) {
			Err.prln("cannot initialise GATE...");
			gex.printStackTrace();
			return;
		}
		
		//createDataStore();
		populateDataStore(4627, 5300);
	}

	public static void populateDataStore(int start, int end) {
		try {
			String f = new File(DS_DIR).toURI().toURL().toString();
			SerialDataStore sds = new SerialDataStore(f);
			sds.open();
			
			File folder = new File(DOC_DIR);
			File[] fileList = folder.listFiles();
			for (int i = 0; i < fileList.length; i++) {
				if (fileList[i].isFile()) {
					if ((i >= start) && (i <= end)) {
						System.out.println(Integer.toString(i)+ " " + fileList[i].toURI().toURL());
					    Document doc = Factory.newDocument(fileList[i].toURI().toURL(), "utf-8");
		                Document persistDoc = null;
		                persistDoc = (Document)sds.adopt(doc,null);
		                sds.sync(persistDoc);
		                assert doc != null;
		                Factory.deleteResource(doc);
		                Factory.deleteResource(persistDoc);
					}
				} else if (fileList[i].isDirectory()) {
					System.out.println("Is a directory; ignoring");
				}
			}
			sds.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public static void createDataStore() {
		try {
			SerialDataStore sds  = (SerialDataStore)Factory.createDataStore("gate.persist.SerialDataStore",
					"file://" + DS_DIR);
			sds.open();

			File folder = new File(DOC_DIR);
			File[] fileList = folder.listFiles();
			for (int i = 0; i < fileList.length; i++) {
				if (fileList[i].isFile()) {
					System.out.println(Integer.toString(i) + " " + fileList[i].toURI().toURL());
				    Document doc = Factory.newDocument(fileList[i].toURI().toURL(), "utf-8");
	                Document persistDoc = null;
	                persistDoc = (Document)sds.adopt(doc,null);
	                sds.sync(persistDoc);
	                assert doc != null;
	                Factory.deleteResource(doc);
	                Factory.deleteResource(persistDoc);
				} else if (fileList[i].isDirectory()) {
					System.out.println("Is a directory; ignoring");
				}
			}
			sds.close();
		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
	}
}
