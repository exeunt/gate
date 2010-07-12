
/*
 * Iterate over a corpus, extracting the first document type annotation, then
 * adding it to the document features.
 */

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import gate.Annotation;
import gate.AnnotationSet;
import gate.Corpus;
import gate.DataStore;
import gate.Document;
import gate.Factory;
import gate.FeatureMap;
import gate.Gate;
import gate.creole.ResourceInstantiationException;
import gate.persist.PersistenceException;
import gate.persist.SerialDataStore;
import gate.security.SecurityException;
import gate.util.ExtensionFileFilter;
import gate.util.GateException;
import gate.util.Out;

public class DocumentCategorizationApp {
	
	private static final String DS_DIR = "C:\\Documents and Settings\\slee\\Desktop\\Filing Data Store";
	private static final String DOC_DIR = "C:\\Documents and Settings\\slee\\Desktop\\raw data sample"; 
	
	public static void main(String[] args) throws ResourceInstantiationException {
		
		try {
			Gate.init();
		}
		catch (GateException gex) {
			gex.printStackTrace();
		}
		
		DocumentCategorizationApp app = new DocumentCategorizationApp();
				
		try {
			//tagCorpusDocuments(corp);
			//saveCorpus(corp, sds);
			SerialDataStore sds = openSerialDataStore();
			Corpus corp = createAndLoadCorpus("Another Filing Corpus");
			//Corpus corp = loadCorpus("Filings___1274301616024___8663", sds);
			assert corp != null;
			saveCorpus(corp, sds);
			//categorizeDocuments(corp);
			//sds.sync(corp);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public DocumentCategorizationApp() {
		
	}
	
	public static Corpus categorizeDocuments(Corpus corpus) {
	/*
	 * Requires corpus with docs tagged with DocumentType, etc. annot. and
	 * document_id features.
	 */
		Iterator iter = corpus.iterator();
		int i;
		while (iter.hasNext()) {
			Document doc = (Document) iter.next();
			AnnotationSet annSet = doc.getAnnotations(); 
			String type = "DocumentType";
			AnnotationSet typeSet = annSet.get(type); //get DocumentType annotations
			List typeList = new ArrayList(typeSet);
			Collections.sort(typeList, new gate.util.OffsetComparator()); //sort annotations from 1st to last
			Iterator typeIter = typeList.iterator(); 
			if (typeIter.hasNext()) {
				Annotation anno = (Annotation)typeIter.next(); //grab 1st annotation
				FeatureMap map = anno.getFeatures();
				String doctype = (String)map.get("type");
				FeatureMap docFeatureMap = doc.getFeatures();
				Out.prln(docFeatureMap.get("document_id"));
				docFeatureMap.put("document_type", doctype);
				doc.setFeatures(docFeatureMap);
			}
			Factory.deleteResource(doc);
		}
		return corpus;
	}
	
	public static void compareDocumentType() {
	/*
	 * Looks at document_type feature of docs and compare to document type
	 * we flag it with in our SQL database.
	 */
	}
	
	public static SerialDataStore openSerialDataStore() throws PersistenceException, MalformedURLException {
		SerialDataStore sds;
		File dir = new File(DS_DIR);
		sds = new SerialDataStore(dir.toURI().toURL().toString());
		sds.open();
		return sds;
	}
	
	public static void saveCorpus(Corpus corpus, SerialDataStore sds) {
		try {
			
			Corpus corp = (Corpus)sds.adopt(corpus, null);
			sds.sync(corp);
			sds.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void tagCorpusDocuments(Corpus corpus) {
		/*
		 * Tags corpus documents with document_id attribute, with docid derived
		 * from file name.
		 */
		Iterator iter = corpus.iterator();
		while(iter.hasNext()) {
			Document doc = (Document) iter.next();
			FeatureMap map = doc.getFeatures();
			String sourceURL = (String) map.get("gate.SourceURL");
			int end = sourceURL.indexOf(".htm");
			int start = sourceURL.lastIndexOf("/") + 1;
			String id = sourceURL.substring(start, end);
			map.put("document_id", id);
			doc.setFeatures(map);
		}
	}
	
	public static Corpus loadCorpus(String corpusID, SerialDataStore sds) throws ResourceInstantiationException {
		//Load corpus from given SerialDataStore and corpusID
		FeatureMap corpFeatures = Factory.newFeatureMap();
		corpFeatures.put(DataStore.LR_ID_FEATURE_NAME, corpusID);
		corpFeatures.put(DataStore.DATASTORE_FEATURE_NAME, sds);
		Corpus persistCorp = (Corpus)Factory.createResource("gate.corpora.SerialCorpusImpl", corpFeatures);
		return persistCorp;
	}
	
	public static Corpus createAndLoadCorpus(String name) throws ResourceInstantiationException, IOException {
		Corpus corpus = null;
		try {
			corpus = Factory.newCorpus(name);
			File directory = new File(DOC_DIR); 
			ExtensionFileFilter filter = new ExtensionFileFilter("htm files", "htm"); 
			URL url = directory.toURI().toURL(); 
			corpus.populate(url, filter, null, false);
		} catch (Exception e) {
			e.printStackTrace();
		} 
		return corpus;
	}
	
}
