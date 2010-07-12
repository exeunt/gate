package examples;

/**
 * <p>Title Indexing and Navigation of Knowledge</p>
 * <p>Copyright: Copyright (c) 2003</p>
 * @author Marin Dimitrov
 * @version 1.0
 */

import java.net.*;
import java.io.*;

import gate.*;
import gate.util.*;
import gate.security.*;

public class CorpusUpload {

  public static final boolean DEBUG = true;

  public static final String DATASTORE_TYPE = "gate.persist.OracleDataStore";
  public static final String DATASTORE_URL = "jdbc:oracle:thin:GATEUSER/gate@192.168.128.208:1521:gate07";
  public static final String CORPUS_NAME = "ink-test-05";
  public static final String INPUT_FOLDER = "c:/test";

  public static final String GATE_USER = "kalina";
  public static final String GATE_GROUP = "English Language Group";
  public static final String GATE_PASSWORD = "sesame";

  public CorpusUpload() {
  }

  public static void main(String[] args) {

    DataStore ds = null;

    //init gate
    try {
      Gate.init();
    }
    catch (GateException gex) {
      Err.prln("Cannot initialuze GATE...");
      gex.printStackTrace();
      return;
    }

    try {
      //open datastore
      ds = Factory.openDataStore(DATASTORE_TYPE, DATASTORE_URL);
      ds.open();

      //get security factory
      //the security factory should be initialised with the same JDBC url as the datastore
      //that's where the user/group information resides
      AccessController ac = Factory.createAccessController(DATASTORE_URL);
      ac.open();

      //login and get session
      User usr = ac.findUser(GATE_USER);
      Group grp = ac.findGroup(GATE_GROUP);
      Session usrSession = ac.login(usr.getName(),GATE_PASSWORD,grp.getID());
      assert ac.isValidSession(usrSession);

      //use this session for all consequent operations with the datastore
      ds.setSession(usrSession);

      //create a temporary transient corpus
      Corpus transientCorpus = Factory.newCorpus(CORPUS_NAME);

      //create access permissions for the new persistent corpus and documents that will be
      //added to the databae datastore
      //use WORLD READ / GROUP WRITE access for this corpus and documents
      SecurityInfo si = new SecurityInfo(SecurityInfo.ACCESS_WR_GW,usr,grp);

      //save the transient corpus into datastore
      //and get back a reference to the persistent corpus
      Corpus persistentCorpus = (Corpus)ds.adopt(transientCorpus,si);

      //now unload the remporary transient corpus since we don't need it anymore
      //all subsequent actions will be performed with the persistent corpus
      Factory.deleteResource(transientCorpus);

      File inputDirectory = new File(INPUT_FOLDER);
      assert inputDirectory.exists() && inputDirectory.isDirectory();

      //get start time for benchmark
      long startTimeMillis = System.currentTimeMillis();

      //get input folder content
      String[] fileNamesArr = inputDirectory.list();

      //iterate file list to create GATE documents
      for (int i=0; i< fileNamesArr.length; i++) {
        File currFile = new File(inputDirectory, fileNamesArr[i]);
        URL currFileLocation = currFile.toURL();

        //create a transient document for the current file
        Document transDoc = Factory.newDocument(currFileLocation);
        assert null != transDoc;

        //save the transient doc in the datastore and get back a reference to the
        //peristent doc
        //use the access permissions created above
        Document persistDoc = (Document)ds.adopt(transDoc,si);
        assert null != persistDoc;

        //unload transient doc since we don't need it
        Factory.deleteResource(transDoc);

        //add persistent doc to the persistent corpus
        //it's still a standalone document in the datastore
        persistentCorpus.add(persistDoc);

        //sync corpus and unload persistent document from memory
        //since we won't process it now - we just want to save it
        persistentCorpus.sync();
        Factory.deleteResource(persistDoc);
      }

      printTime(startTimeMillis, fileNamesArr.length);
      Out.prln("Done...");
    }
    catch (Exception ex) {
      Err.prln("Exception caught...");
      ex.printStackTrace();
    }
  }

  private static void printTime(long startTimeM, long files) {

    long hours, mins, secs;
    long currTimeMillis = System.currentTimeMillis();
    long ct = currTimeMillis / 1000;
    long st = startTimeM / 1000;

    hours = (ct - st) / 3600;
    mins = ((ct - st) % 3600) / 60;
    secs = ((ct - st) % 3600) % 60;

    Out.prln("["+ files +"] files uploaded in "+ hours +"h "+ mins +"m "+ secs +"s");
  }

}
 