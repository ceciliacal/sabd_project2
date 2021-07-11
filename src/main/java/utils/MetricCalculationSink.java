package utils;


import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class MetricCalculationSink implements SinkFunction<String> {

    private static long count = 0L;
    private static long startTime = 0L;

    //quando viene vista una nuova tupla, aumento contatore
    public static synchronized void calculateMetrics() {

        if (startTime == 0L) {
            startTime = System.currentTimeMillis();
            System.out.println("---vista prima tupla");
            return;
        }


        count++;
        //tempo corrente
        double currentTime = System.currentTimeMillis() - startTime;
        currentTime = currentTime/1000;

        double throughput = (count/currentTime);
        double latency = (currentTime/count);

        //throughput e latenza calcolate finora
        System.out.println("throughput: " + throughput);
        System.out.println("latency: " + latency);
    }

    @Override
    public void invoke(String value, Context context) {
        calculateMetrics();
    }
}
