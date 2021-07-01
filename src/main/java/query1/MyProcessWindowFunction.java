package query1;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;

public class MyProcessWindowFunction extends ProcessWindowFunction {
    @Override
    public void process(Object o, Context context, Iterable iterable, Collector collector) throws Exception {

    }
}
