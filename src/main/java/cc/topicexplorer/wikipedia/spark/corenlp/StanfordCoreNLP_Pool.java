package cc.topicexplorer.wikipedia.spark.corenlp;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;

public class StanfordCoreNLP_Pool extends GenericObjectPool<StanfordCoreNLP>{

	public StanfordCoreNLP_Pool(PooledObjectFactory<StanfordCoreNLP> factory, GenericObjectPoolConfig config) {
		super(factory, config);
	}
}
