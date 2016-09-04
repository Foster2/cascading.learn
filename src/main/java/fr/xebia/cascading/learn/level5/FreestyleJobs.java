package fr.xebia.cascading.learn.level5;

import cascading.flow.FlowDef;
import cascading.operation.Insert;
import cascading.operation.aggregator.First;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.*;
import cascading.pipe.assembly.*;
import cascading.pipe.joiner.InnerJoin;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * You now know all the basics operators. Here you will have to compose them by yourself.
 */
public class FreestyleJobs {

    /**
     * Word count is the Hadoop "Hello world" so it should be the first step.
     * <p>
     * source field(s) : "line"
     * sink field(s) : "word","count"
     */
    public static FlowDef countWordOccurences(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {

        Pipe pipe = new Pipe("countWordOccurences");

        RegexSplitGenerator splitter = new RegexSplitGenerator(new Fields("word"), "[ \\[\\]\\(\\),.'/]");

        pipe = new Each(pipe, new Fields("line"), splitter, Fields.SWAP);

        pipe = new Each(pipe, new Fields("word"), new ExpressionFunction(new Fields("word"), "word.trim().toLowerCase()", String.class));

        pipe = new Each(pipe, new Fields("word"), new ExpressionFilter("word.trim().length()==0", String.class));

        pipe = new Each(pipe, new Fields("word"), new RegexFilter("\\w*\\d\\w*", true));


        Fields groupingFields = new Fields("word");
        Fields countField = new Fields("count");

        pipe = new CountBy(pipe, groupingFields, countField);

        return FlowDef.flowDef()//
                .addSource(pipe, source) //
                .addTail(pipe)//
                .addSink(pipe, sink);
    }

    /**
     * Now, let's try a non trivial job : td-idf. Assume that each line is a
     * document.
     * <p>
     * source field(s) : "line"
     * sink field(s) : "docId","tfidf","word"
     * <p>
     * <pre>
     * t being a term
     * t' being any other term
     * d being a document
     * D being the set of documents
     * Dt being the set of documents containing the term t
     *
     * tf-idf(t,d,D) = tf(t,d) * idf(t, D)
     *
     * where
     *
     * tf(t,d) = f(t,d) / max (f(t',d))
     * ie the frequency of the term divided by the highest term frequency for the same document
     *
     * idf(t, D) = log( size(D) / size(Dt) )
     * ie the logarithm of the number of documents divided by the number of documents containing the term t
     * </pre>
     * <p>
     * Wikipedia provides the full explanation
     *
     * @see http://en.wikipedia.org/wiki/tf-idf
     * <p>
     * If you are having issue applying functions, you might need to learn about field algebra
     * @see http://docs.cascading.org/cascading/3.0/userguide/ch04-tuple-fields.html#field-algebra
     * <p>
     * {@link First} could be useful for isolating the maximum.
     * <p>
     * {@link HashJoin} can allow to do cross join.
     * <p>
     * PS : Do no think about efficiency, at least, not for a first try.
     * PPS : You can remove results where tfidf < 0.1
     */
    public static FlowDef computeTfIdf(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {

        // not exactly the same but basically likewise

        Pipe pipe = new Pipe("computeTfIdf");

        RegexSplitGenerator splitter = new RegexSplitGenerator(new Fields("word"), "[ \\[\\]\\(\\),.'/]");


        pipe = new Each(pipe, new Fields("content"), splitter, Fields.SWAP);

        pipe = new Each(pipe, new Fields("word"), new ExpressionFilter("word.trim().length()==0", String.class));

        pipe = new Each(pipe, new Fields("word"), new ExpressionFunction(new Fields("word"), "word.trim().toLowerCase()", String.class), Fields.SWAP);

        pipe = new Each(pipe, new Fields("word"), new RegexFilter("\\w*\\d\\w*", true));

        pipe = new Rename(pipe,new Fields("id"),new Fields("docId"));



        Fields groupingFields = new Fields("docId", "word" );
        Fields countField = new Fields( "count" );

        pipe = new CountBy(pipe, groupingFields, countField );


        // term frequency computation
        Pipe pipeFreqWordPerDoc = new Pipe( "pipeFreqWordPerDoc", pipe );
        pipeFreqWordPerDoc = new MaxBy(pipeFreqWordPerDoc, new Fields("docId"), new Fields( "count" ), new Fields("freqWordPerDoc")  );


        Fields declared = new Fields("docId","word","count","docId2","freqWordPerDoc");
        Pipe termFreq = new CoGroup(pipe,new Fields("docId" ), pipeFreqWordPerDoc,new Fields("docId"),declared,new InnerJoin() );

        termFreq = new Discard(termFreq,new Fields("docId2"));
        termFreq =  new Each( termFreq, new Fields("count","freqWordPerDoc"),new ExpressionFunction( new Fields("termFreq"), "count/freqWordPerDoc", Double.class ), Fields.ALL );


        // doc frequency computation
        Pipe pipeDocFreq = new Pipe( "pipeDocFreq", pipe );
        pipeDocFreq = new CountBy(pipeDocFreq, new Fields("word"), new Fields( "docId" ), new Fields("docFreq")  );
// 'docId	word	count	freqWordPerDoc	termFreq 'word	docFreq
        Fields declared2 = new Fields("docId","word","count","freqWordPerDoc","termFreq","word2","docFreq");
        pipeDocFreq = new CoGroup(termFreq,new Fields("word" ),pipeDocFreq ,new Fields("word"),declared2,new InnerJoin() );
//
        pipeDocFreq =  new Each( pipeDocFreq, new Fields("termFreq","docFreq"),new ExpressionFunction( new Fields("tfidf"), "termFreq*Math.log( (double) 5 / ( 0 + docFreq ) )", Double.class ), Fields.ALL );

        pipeDocFreq = new Each(pipeDocFreq, new Fields("tfidf"), new ExpressionFilter("tfidf<0.1", double.class));


        pipeDocFreq = new Retain(pipeDocFreq,new Fields("docId", "tfidf","word"));

        pipeDocFreq = new GroupBy(pipeDocFreq,Fields.ALL,new Fields("docId","tfidf"),true);


        return FlowDef.flowDef()//
                .addSource(pipe, source) //
                .addTail(pipeDocFreq)//
                .addSink(pipeDocFreq, sink);
    }

}
