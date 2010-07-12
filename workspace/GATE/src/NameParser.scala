import java.util._
import java.io._
import java.net._

import _root_.gate._
import _root_.gate.creole._
import _root_.gate.util._


object NameParser {
	def main(args: Array[String]): Unit = {
		Gate.init()
	    val gateHome = Gate.getGateHome()
	
	    var corpus: Corpus = Factory.createResource("gate.corpora.CorpusImpl").asInstanceOf[Corpus]
		val docFile = new File("""C:\My Dropbox\GATE\Data Samples\Manager Section""")
		corpus.populate(docFile.toURL(), null, "utf-8", false)
		println(corpus.getDocumentNames())
	}
}