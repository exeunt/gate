
import gate.Corpus;
import gate.Document;
import gate.Factory;
import gate.Gate;
import gate.creole.ResourceInstantiationException;
import gate.persist.PersistenceException;
import gate.persist.SerialDataStore;
import gate.security.SecurityException;
import gate.util.Err;
import gate.util.GateException;
import gate.util.Out;
import java.io.File;
import java.net.MalformedURLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
//import java.util.logging.Level;
//import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class CorpusBuilder {

	private static Log log = LogFactory.getLog(CorpusBuilder.class);

	//the directory must EXIST and be EMPTY
	private static final String DS_DIR = "ds_test";
	//private String dataStoreDir = DS_DIR;
	private File dataStoreDir = new File(DS_DIR);

	public enum Action {CREATE, ADD, LIST};
	private Action action;

	private String corpusName;

	public String getAction() {
		return action.toString();
	}

	public void setAction(String action) {
		this.action = Action.valueOf(action.toUpperCase());
	}

	public String getCorpusName() {
		return corpusName;
	}

	public void setCorpusName(String corpusName) {
		this.corpusName = corpusName;
	}


	private gate.DataStore dataStore;
	private boolean isOpen = false;

	public String getDataStoreDir() {
		return dataStoreDir.getAbsolutePath();
	}

	public void setDataStoreDir(String dataStoreDir) {
		if (isOpen) { this.close();}
		//this.dataStoreDir = dataStoreDir;
		//this.dataStoreDir = (new File(dataStoreDir)).toURI().toString();
		this.dataStoreDir = new File(dataStoreDir);
		//Err.println("Set DataStore URI:"+this.dataStoreDir);
		this.open();
	}

	public CorpusBuilder() {
	}

	/**
	 * @param args the command line arguments
	 * 	- datastore : camino al datastore
	 *  - orden :
	 *    - 'create' : crear un nuevo corpus (error si ya existe)
	 *    - 'add' : añadir los documentos
	 *    - 'list' : listar los documentos del corpus
	 *  - corpus : nombre del corpus
	 *  - doc* : documeto(s) a añadir/listar
	 */
	public static void main(String[] args) {
		try {
			Gate.init();
		}
		catch (GateException gex) {
			Err.prln("cannot initialise GATE...");
			gex.printStackTrace();
			return;
		}

		CorpusBuilder cp = new CorpusBuilder();
		cp.setDataStoreDir(args[0]); // carpeta del datastore
		cp.setAction(args[1]);
		//TODO : Probar si el action es valida y si tiene el buen numero de argumentos
		if (args.length > 2) cp.setCorpusName(args[2]);
		cp.run(args.length > 3 ? Arrays.copyOfRange(args, 3, args.length) : new String[] {});
	}




	private void close() {
		if (isOpen) {
			try {
				dataStore.close();
			} catch (PersistenceException ex) {
				log.error(null, ex);
			}
		}
		isOpen=false;
	}

	private void open() {
		if (! isOpen) {
			try {
				//TODO : probar/crear carpeta
				//File dsDir = new File(dataStoreDir);
				if (! dataStoreDir.isDirectory()) {
					if (! dataStoreDir.mkdirs()) {
						log.fatal(MessageFormat.format("No se puede crear la carpeta del DataStore \"{0}\"", dataStoreDir));
						System.exit(1);
					}
					dataStore = (SerialDataStore) Factory.createDataStore("gate.persist.SerialDataStore", this.dataStoreDir.toURI().toString());
				}
				else {
					try {
						dataStore = (SerialDataStore) Factory.openDataStore("gate.persist.SerialDataStore", this.dataStoreDir.toURI().toString());
					}
					catch (PersistenceException e) {
						dataStore = (SerialDataStore) Factory.createDataStore("gate.persist.SerialDataStore", this.dataStoreDir.toURI().toString());

					}
				}
				//dataStore = (SerialDataStore) Factory.createDataStore("gate.persist.SerialDataStore", this.dataStoreDir);
				isOpen=true;
			} catch (PersistenceException ex) {
				log.error(null, ex);
			} catch (UnsupportedOperationException ex) {
				log.error(null, ex);
			}
		}
		if (! isOpen) { System.exit(1);}
	}

	private void run(String[] docs) {
		open(); // Abrir el datastore
		Corpus corp;
		switch (action) {
		case CREATE :
			if (findDSCorpus(false) != null) {
				log.fatal(MessageFormat.format("[CREATE] ERROR : el corpus \"{0}\" ya existe", corpusName));
				return;
			}
			corp = createDSCorpus(); // crear el corpus
			addDocsInCorpus(corp, docs);
			break;
		case ADD :
			corp = findDSCorpus(false);
			if (corp == null) {
				log.fatal(MessageFormat.format("[ADD] ERROR : el corpus \"{0}\" no existe", corpusName));
				return;
			}
			addDocsInCorpus(corp, docs);
			break;
		case LIST :
			List<Corpus> corpuses = matchDSCorpus();
			//	corp = findDSCorpus(false);
			if (corpuses.isEmpty()) {
				log.fatal(MessageFormat.format("[LIST] ERROR : ningún corpus corresponde a \"{0}\"", corpusName));
				return;
			}
			for (Corpus c : corpuses) {
				log.debug(MessageFormat.format("[LIST] : Documentos del corpus \"{0}\"", c.getName()));
				for (Object cdoc : c) {
					Out.println(((Document) cdoc).getName());
				}
			}
			break;
		}
	}

	/**
	 * Añadir uno(s) documento(s) a un corpus.
	 * @param corp
	 * @param docs
	 */
	public void addDocsInCorpus(Corpus corp, String[] docs) {
		try {
			// añadir los documentos
			for (String docPath : docs) {
				File docFile = new File(docPath);
				// Crear el nuevo documento (a partir de su URL)
				Document doc = Factory.newDocument(docFile.toURI().toURL());
				// Cambiar el nombre por el 'basename' (sin extensión)
				doc.setName(docFile.getName().replaceFirst("\\.\\w+$", ""));
				// Elegir unas opciones : mark-up, preserve origin, ...
				doc.setMarkupAware(true);
				doc.setPreserveOriginalContent(true);
				// Añadir el documento al corpus
				corp.add(doc);
			}
			dataStore.sync(corp); // sincronisar
		} catch (MalformedURLException ex) {
			log.error(null, ex);
		} catch (PersistenceException ex) {
			log.error(null, ex);
		} catch (SecurityException ex) {
			log.error(null, ex);
		} catch (ResourceInstantiationException ex) {
			log.error(null, ex);
		}

	};

	/**
	 * Buscar el corpus en el Datastore.
	 * @param create	Crearlo si no existe.
	 * @return	El corpus o null si no se encontra en el Datastore
	 */
	public Corpus findDSCorpus(boolean create) {
		open();
		try {
			String cclass = "gate.corpora.SerialCorpusImpl";//gate.corpora.SerialCorpusImpl.class.toString();
			@SuppressWarnings("unchecked")
			List<Object> corpusIds = dataStore.getLrIds(cclass);
			for (Object cid : corpusIds) {
				if (dataStore.getLrName(cid).equals(corpusName)) {
					try {
						return (Corpus) dataStore.getLr(cclass, cid);
					} catch (SecurityException ex) {
						log.error(null, ex);
					}
				}
			}
			// no se encuentra
			if (create) { return createDSCorpus(); }
		} catch (PersistenceException ex) {
			log.error(null, ex);
		}
		return null;
	}

	/**
	 * Buscar los corpus en el Datastore.
	 * @return	Los corpus que corresponden en el Datastore
	 *	o una lista vacía.
	 */
	public List<Corpus> matchDSCorpus() {
		List<Corpus> res = new ArrayList<Corpus>();
		Pattern corpusPattern = Pattern.compile(corpusName != null ? corpusName : ".*");
		open();
		try {
			String cclass = "gate.corpora.SerialCorpusImpl";//gate.corpora.SerialCorpusImpl.class.toString();
			@SuppressWarnings("unchecked")
			List<Object> corpusIds = dataStore.getLrIds(cclass);
			for (Object cid : corpusIds) {
				Matcher m = corpusPattern.matcher(dataStore.getLrName(cid));
				if (m.matches()) {
					try {
						Corpus c = (Corpus) dataStore.getLr(cclass, cid);
						res.add(c);
					} catch (SecurityException ex) {
						log.error(null, ex);
					}
				}
			}
		} catch (PersistenceException ex) {
			log.error(null, ex);
		}
		return res;
	}

	protected Corpus createDSCorpus() {
		try {
			Corpus corp = Factory.newCorpus(corpusName);
			assert corp != null;
			Corpus pcorp = (Corpus) dataStore.adopt(corp, null);
			dataStore.sync(pcorp);
			return pcorp;
		} catch (PersistenceException ex) {
			log.error(null, ex);
		} catch (SecurityException ex) {
			log.error(null, ex);
		} catch (ResourceInstantiationException ex) {
			log.error(null, ex);
		}
		return null;
	}

}
