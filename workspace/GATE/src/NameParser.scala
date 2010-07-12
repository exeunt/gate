import java.util._
import java.io._
import java.net._

import _root_.gate._
import _root_.gate.creole._
import _root_.gate.util._


object NameParser {
	def main(args: Array[String]): Unit = {
		println("Weee")
		Gate.init()

	    // Load ANNIE plugin
	    val gateHome = Gate.getGateHome()
	    val pluginsHome = new File(gateHome, "plugins")
	    Gate.getCreoleRegister().registerDirectories(new File(pluginsHome, "ANNIE").toURL())
	

	    // create a GATE corpus and add a document for each command-line
	    // argument
	    var corpus: Corpus = Factory.createResource("gate.corpora.CorpusImpl").asInstanceOf[Corpus]
		
		val docFile = new File("""C:\My Dropbox\GATE\Datastore\gate.corpora.DocumentImpl""")
		corpus.populate(docFile.toURL(), null, "utf-8", false)
		println(corpus.getDocumentNames())
	}

}