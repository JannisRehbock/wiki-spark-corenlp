package cc.topicexplorer.wikipedia.spark.corenlp;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.CoreSentence;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class WikiSparkCorenlp {

	private static StanfordCoreNLP_Pool pool;
	
	public static void main(String[] args) {
		if (args.length != 4) {
			System.out.println("Number of parameters is wrong.");
			System.out.println("For usage with maven you need to two commands: \n"
					+ "first set jvm parameters of maven,\n"
					+ "second excute main class with parameters :");
			System.out.println("export MAVEN_OPTS=\"-Xmx<amount of main memory for wikipedia processing with spark and corenlp, e.g. 80>G\" && \\");
			System.out.println("mvn exec:java -Dexec.mainClass=\"cc.topicexplorer.wikipedia.spark.corenlp.WikiSparkCorenlp\" \\\n"
					+ "-Dexec.args=\""
					+ "<path/to/data/file> "
					+ "<path/to/output/file> "
					+ "<number of local parallel worker threads> "
					+ "<partition factor>\"");			
			return;
		}
		
		String csvDataFilePath = args[0];
		String csvOutputFilePath = args[1];
		Integer numWorkers = new Integer(Integer.parseUnsignedInt(args[2]));
		Integer partitionFactor = new Integer(Integer.parseUnsignedInt(args[3])); // number of partitions of data is numWorkers * partitionFaktor

		// Init the coreNLP Pipeline Pool
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        
        config.setMaxIdle(numWorkers+1);
        config.setMaxTotal(numWorkers+1);
        /*---------------------------------------------------------------------+
        |TestOnBorrow=true --> To ensure that we get a valid object from pool  |
        |TestOnReturn=true --> To ensure that valid object is returned to pool |
        +---------------------------------------------------------------------*/
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);

        pool = new StanfordCoreNLP_Pool(new StanfordCoreNLPFactory(), config);

		
		
		String master = "local[*]";

		SparkConf conf = new SparkConf()
				.setAppName(WikiSparkCorenlp.class.getName())
				.setMaster(master);

		SparkSession spark = SparkSession.builder()
				.appName("NLP-Preprocessing of Wikipedia using Spark and CoreNlp")
				.config(conf)
				.getOrCreate();

		// Define the schema of the csv file
		List<StructField> fields = new ArrayList<>();
		fields.add(DataTypes.createStructField("page_id", DataTypes.LongType, true));
		fields.add(DataTypes.createStructField("title", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("revision", DataTypes.LongType, true));
		fields.add(DataTypes.createStructField("type", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("ns_id", DataTypes.IntegerType, true));
		fields.add(DataTypes.createStructField("ns_name", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("text", DataTypes.StringType, true));
		StructType wikiSchema = DataTypes.createStructType(fields);

		// "/data/alex/Wikipedia/enwiki-20180220-pages-articles_filtered_ns0.csv"
		Dataset<Row> wikiDFCsv = spark.read().format("csv")
				.option("sep", ",")
				.option("header", "false")
				.option("mode", "FAILFAST")
				.option("multiLine", "true")
				.schema(wikiSchema)
				.load(csvDataFilePath);
		
		Dataset<Row> wikiDF_partioned = wikiDFCsv.repartition(numWorkers * partitionFactor).cache();
//		wikiDF_partioned.show();
		
		Encoder<ExtractedToken> extractedTokenClassEncoder = Encoders.bean(ExtractedToken.class);
		Dataset<ExtractedToken> wikiDF_nlp = wikiDF_partioned.flatMap(nlpProcessing,extractedTokenClassEncoder);
						
//		wikiDF_nlp.show(20,false);
		wikiDF_nlp.write().format("csv")
		        .option("sep", ",")
		        .option("header", "true")
		        .save(csvOutputFilePath);
		
		spark.stop();
		pool.close();
	}
	
    private static FlatMapFunction<Row, ExtractedToken> nlpProcessing = new FlatMapFunction<Row,  ExtractedToken>() {
		private static final long serialVersionUID = -8966257265832650600L;

		public Iterator<ExtractedToken> call(final Row wikipediaPage) throws Exception {
			ArrayList<ExtractedToken> result = new ArrayList<ExtractedToken>();
			
			Long pageId = wikipediaPage.getLong(0);

			StanfordCoreNLP pipeline  = null;
            try {

                pipeline = pool.borrowObject();

                // Here we could count how often a object is borrowed
                // from the example of the pool                
                // count.getAndIncrement();
                
    	    	CoreDocument document = new CoreDocument(wikipediaPage.getString(6)); //wikipediaPageText);
    	    	// annnotate the document
    	    	pipeline.annotate(document);
    	    	
    	    	List<CoreSentence> sentences = document.sentences();
    	    	Integer sentNo = new Integer(0);
    	    	for(CoreSentence sent: sentences) {
    	    	
            		List<CoreLabel> token = sent.tokens(); Iterator<CoreLabel> tokenIt = token.iterator();
              		List<String> pos = sent.posTags(); Iterator<String> posIt = pos.iterator();
//             		SemanticGraph depGraph = sent.dependencyParse(); 
//             		List<SemanticGraphEdge> depEdges = depGraph.edgeListSorted();         		
//             		Iterator<SemanticGraphEdge> depEdgeIt = depEdges.iterator();
//            		List<String> ner = sent.nerTags(); Iterator<String> nerIt = ner.iterator();
            		
        	    	Integer tokenNo = new Integer(0);
                	while (    tokenIt.hasNext() 
                			&& posIt.hasNext() 
//                			&& depEdgeIt.hasNext()
//                			&& nerIt.hasNext()
                		  ) 
                	{ 
                		CoreLabel x = tokenIt.next();
                		ExtractedToken extractedToken = new ExtractedToken();
                		extractedToken.setPageId(pageId);
                		extractedToken.setSentenceNo(sentNo);
                		extractedToken.setTokenNo(tokenNo);

                		extractedToken.setToken(x.originalText());
                		extractedToken.setBeginPosition(x.beginPosition());
                		extractedToken.setEndPosition(x.endPosition());
                		extractedToken.setLemma(x.lemma());
                		extractedToken.setPos(posIt.next());
                		

                		// Not used because those attributes are expensive
//                		extractedToken.setNer(x.ner()); // or use nerIt.next()                		
//                		SemanticGraphEdge y = depEdgeIt.next();
//                		extractedToken.setEdge(y.toString());
                		
                		result.add(extractedToken);
                		tokenNo++;
                	}
                	sentNo++;
    	    	}
            } catch (Exception e) {
                e.printStackTrace(System.err);
            } finally {
                if (pipeline != null) {
                    pool.returnObject(pipeline);
                }
            }
            return result.iterator();
        }
    };

}