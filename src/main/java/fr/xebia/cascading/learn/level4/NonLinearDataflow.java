package fr.xebia.cascading.learn.level4;

import cascading.flow.FlowDef;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.joiner.InnerJoin;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Up to now, operations were stacked one after the other. But the dataflow can
 * be non linear, with multiples sources, multiples sinks, forks and merges.
 */
public class NonLinearDataflow {
	
	/**
	 * Use {@link CoGroup} in order to know the party of each presidents.
	 * You will need to create (and bind) one Pipe per source.
	 * You might need to correct the schema in order to match the expected results.
	 * 
	 * presidentsSource field(s) : "year","president"
	 * partiesSource field(s) : "year","party"
	 * sink field(s) : "president","party"
	 * 
	 * @see http://docs.cascading.org/cascading/3.0/userguide/ch05-pipe-assemblies.html#_cogroup
	 */
	public static FlowDef cogroup(Tap<?, ?, ?> presidentsSource, Tap<?, ?, ?> partiesSource,
			Tap<?, ?, ?> sink) {
		Pipe lhs = new Pipe("lhs");
		Pipe rhs = new Pipe("rhs");

		Fields declared = new Fields(
				"year1","president", "year2","party"
		);

		Pipe pipe = new CoGroup( lhs, new Fields("year" ), rhs, new Fields("year"),declared,new InnerJoin() );

		pipe = new Discard(pipe, new Fields("year1","year2"));


		return FlowDef.flowDef()//
				.addSource(lhs, presidentsSource) //
				.addSource(rhs, partiesSource) //
				.addTail(pipe)//
				.addSink(pipe, sink);
	}
	
	/**
	 * Split the input in order use a different sink for each party. There is no
	 * specific operator for that, use the same Pipe instance as the parent.
	 * You will need to create (and bind) one named Pipe per sink.
	 * 
	 * source field(s) : "president","party"
	 * gaullistSink field(s) : "president","party"
	 * republicanSink field(s) : "president","party"
	 * socialistSink field(s) : "president","party"
	 * 
	 * In a different context, one could use {@link PartitionTap} in order to arrive to a similar results.
	 * @see http://docs.cascading.org/cascading/3.0/userguide/ch15-advanced.html#partition-tap
	 */
	public static FlowDef split(Tap<?, ?, ?> source,
			Tap<?, ?, ?> gaullistSink, Tap<?, ?, ?> republicanSink, Tap<?, ?, ?> socialistSink) {

		Pipe gaullistPipe = new Pipe("gaullistPipe");
		Pipe republicanPipe = new Pipe("republicanPipe");
		Pipe socialistPipe = new Pipe("socialistPipe");

		gaullistPipe = new Each(gaullistPipe,new Fields("party"),new ExpressionFilter("!party.equals(\"Gaullist\")",String.class));

		republicanPipe = new Each(republicanPipe,new Fields("party"),new ExpressionFilter("!party.equals(\"Republican\")",String.class));

		socialistPipe = new Each(socialistPipe,new Fields("party"),new ExpressionFilter("!party.equals(\"Socialist\")",String.class));

		return FlowDef.flowDef()//
				.addSource(gaullistPipe, source) //
				.addSource(republicanPipe,source)//
				.addSource(socialistPipe,source)//
				.addTail(gaullistPipe)
				.addTail(republicanPipe)
				.addTail(socialistPipe)
				.addSink(gaullistPipe, gaullistSink)
		.addSink(republicanPipe, republicanSink)
		.addSink(socialistPipe, socialistSink);

	}
	
}
