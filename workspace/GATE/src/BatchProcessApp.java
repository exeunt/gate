

import gate.Document;
import gate.Corpus;
import gate.CorpusController;
import gate.AnnotationSet;
import gate.Gate;
import gate.Factory;
import gate.util.*;
import gate.util.persistence.PersistenceManager;

import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.OutputStreamWriter;

public class BatchProcessApp {
	private static int firstFile = 0;
	private static File gappFile = null;
	private static List annotTypesToWrite = null;
	private static String encoding = "utf-8";

	public static void main(String[] args) throws Exception {
		String[] input = {"-g", "", };

		Gate.init();

		// load the saved application
		CorpusController application =
			(CorpusController)PersistenceManager.loadObjectFromFile(gappFile);

		// Create a Corpus to use.  We recycle the same Corpus object for each
		// iteration.  The string parameter to newCorpus() is simply the
		// GATE-internal name to use for the corpus.  It has no particular
		// significance.
		Corpus corpus = Factory.newCorpus("BatchProcessApp Corpus");
		application.setCorpus(corpus);

		// process the files one by one
		for(int i = 0; i < args.length; i++) {
			// load the document (using the specified encoding if one was given)
			File docFile = new File(args[i]);
			System.out.print("Processing document " + docFile + "...");
			Document doc = Factory.newDocument(docFile.toURI().toURL(), encoding);

			corpus.add(doc);
			application.execute();
			corpus.clear();

			Factory.deleteResource(doc);


		} // for each file

		System.out.println("All done");
	}

}