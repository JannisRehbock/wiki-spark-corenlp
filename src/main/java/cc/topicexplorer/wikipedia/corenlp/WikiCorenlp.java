package cc.topicexplorer.wikipedia.corenlp;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.CoreSentence;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.simple.Document;
import edu.stanford.nlp.simple.Sentence;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

public class WikiCorenlp {
	public static void main(String[] args) throws IOException {
		if (args.length == 0) {
			System.out.println("parameter for data file missing");
			return;
		}
		
		Charset charset = Charset.forName("UTF-8");
		CsvReader wikiReader = new CsvReader(args[0], ',',charset );
		wikiReader.setEscapeMode(CsvWriter.ESCAPE_MODE_BACKSLASH);
		wikiReader.setTextQualifier('"');
		wikiReader.setUseTextQualifier(true);

		
		 // set up pipeline properties
	    Properties props = new Properties();
	    // set the list of annotators to run
	    props.setProperty("annotators", "tokenize,ssplit,pos,lemma");
	    props.setProperty("threads", "1");
	    // build pipeline
	    StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
    	ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>(); 

	    while (wikiReader.readRecord())
		{
	    	// create a document object
		    System.out.println("Dokument Length " + wikiReader.get(6).length());

	    	CoreDocument document = new CoreDocument(wikiReader.get(6));
	    	// annnotate the document
	    	pipeline.annotate(document);
	    	
	    	List<CoreSentence> sentences = document.sentences();
	    	for(CoreSentence sent: sentences) {
	    	
        		List<CoreLabel> token = sent.tokens(); Iterator<CoreLabel> tokenIt = token.iterator();
          		List<String> pos = sent.posTags(); Iterator<String> posIt = pos.iterator();
//        		List<String> ner = sent.nerTags(); Iterator<String> nerIt = ner.iterator();
        		
            	while (tokenIt.hasNext() && posIt.hasNext() ) { //&& nerIt.hasNext()
            		ArrayList<String> parsedToken = new ArrayList<String>(); 
            		CoreLabel x = tokenIt.next();
            		parsedToken.add(x.originalText());
            		parsedToken.add(Integer.toString(x.beginPosition()));
            		parsedToken.add(Integer.toString(x.endPosition()));            		
            		parsedToken.add(x.lemma());
//            		parsedToken.add(x.ner());
            		parsedToken.add(posIt.next());
//            		parsedToken.add(nerIt.next());            	    
            		result.add(parsedToken);
            	}
	    	}
		}
	    System.out.println(result);
	}
}
