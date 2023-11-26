package cn.smileyan.demo.wordcount;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * 统计单词个数
 * @author Smileyan
 */
@Slf4j
public class WordCount {
    /**
     * 默认的用于统计单词个数的字符串
     */
    public static final String DEFAULT_WORDS = "Flink’s Table & SQL API makes it possible to work with queries written " +
            "in the SQL language, but these queries need to be embedded within a table program that is written in either Java or Scala. " +
            "Moreover, these programs need to be packaged with a build tool before being submitted to a cluster. " +
            "This more or less limits the usage of Flink to Java/Scala programmers" +
            "The SQL Client aims to provide an easy way of writing, debugging, and submitting table programs " +
            "to a Flink cluster without a single line of Java or Scala code. " +
            "The SQL Client CLI allows for retrieving and visualizing real-time results from the running distributed " +
            "application on the command line.";

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setParallelism(2);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataStream<String> text = null;
        if (params.has("input")) {
            // union all the inputs from text files
            for (String input : params.getMultiParameterRequired("input")) {
                if (text == null) {
                    text = env.readTextFile(input);
                } else {
                    text = text.union(env.readTextFile(input));
                }
            }
            Preconditions.checkNotNull(text, "Input DataStream should not be null.");
        } else {
            log.info("Executing WordCount example with default input data set.");
            log.info("Use --input to specify file input.");
            // get default test text data
            text = env.fromElements(DEFAULT_WORDS);
        }

        assert text != null;
        DataStream<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .keyBy(value -> value.f0)
                        .sum(1);

        // emit result
        if (params.has("output")) {
            log.info("result is {}", params.get("output"));
        } else {
            log.info("Printing result to stdout. Use --output to specify output path.");
            counts.print();
        }

        // execute program
        env.execute("Streaming WordCount");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into multiple pairs in the
     * form of "(word,1)" ({@code Tuple2<String, Integer>}).
     */
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        private static final long serialVersionUID = 8061659867139246041L;

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (!token.isEmpty()) {
                    out.collect(Tuple2.of(token, 1));
                }
            }
        }
    }
}
