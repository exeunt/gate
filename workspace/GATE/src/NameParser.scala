import scala.collection.mutable.ListBuffer

import java.util._
import java.io._
import java.net._
import java.io.FileReader

import _root_.gate._
import _root_.gate.creole._
import _root_.gate.util._
import _root_.gate.util.persistence.PersistenceManager
import _root_.au.com.bytecode.opencsv.CSVWriter


object NameParser {
	def main(args: Array[String]): Unit = {
		
		val writer = new CSVWriter(new FileWriter("results3.csv"), ',')
		
		Gate.init()
	    val gappfile = new File("""C:\My Dropbox\GATE\gapp\managernameparser.gapp""")
	    val app = PersistenceManager.loadObjectFromFile(gappfile).asInstanceOf[CorpusController]
	    
	    var corpus: Corpus = Factory.createResource("gate.corpora.CorpusImpl").asInstanceOf[Corpus]
		val docFile = new File("""C:\My Dropbox\GATE\Data Samples\Manager Section""")
		corpus.populate(docFile.toURL(), null, "utf-8", "text/plain", false)
		
		app.setCorpus(corpus)
		app.execute()
		val corpiter = corpus.iterator()
		
		while (corpiter.hasNext()) {
			val names = new ListBuffer[String]
			val doc = corpiter.next.asInstanceOf[Document]
			val url = doc.getSourceUrl.getFile.asInstanceOf[String]
			println(url)
			val list = url.split("""/""")
			val listLength = list.length
			val docname = list(listLength - 1) dropRight(7)
			names += docname
			val annots = doc.getAnnotations().asInstanceOf[AnnotationSet]
			val annotIter = annots.get("Person").iterator()
			val content = doc.getContent()
			
			while (annotIter.hasNext()) {
				val annot = annotIter.next()
				val start = annot.getStartNode().getOffset()
				val end = annot.getEndNode().getOffset()
				val name = content.getContent(start, end).toString().trim()
				names += name
				//content.slice(start, end)
			}
			writer.writeNext(names.toArray)
		}
		writer.close
		
		//println(corpus.getDocumentNames())
	}
}