package cc.topicexplorer.wikipedia.spark.corenlp;

import java.util.Properties;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;

public class StanfordCoreNLPFactory
extends BasePooledObjectFactory<StanfordCoreNLP> {

	@Override
	public StanfordCoreNLP create() throws Exception {
		 // set up pipeline properties
	    Properties props = new Properties();
	    
	    // set the list of annotators to run
	    
	    // expensive
	    // props.setProperty("annotators", "tokenize,ssplit,pos,lemma,depparse");
	    
	    // very expensive
	    // props.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner");
	    
	    // cheap and fast
	    props.setProperty("annotators", "tokenize,ssplit,pos,lemma");
	    props.setProperty("threads", "1");
	    // build pipeline
	    return new StanfordCoreNLP(props);
	}

	@Override
	public PooledObject<StanfordCoreNLP> wrap(StanfordCoreNLP pipeline) {
		return new DefaultPooledObject<StanfordCoreNLP>(pipeline);
	}
}
