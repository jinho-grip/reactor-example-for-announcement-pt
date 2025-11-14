package example;

import java.util.concurrent.Flow;

public class C1_CustomMono {

    public static void main(String[] args) {

        class Mono implements Flow.Publisher<Integer> {
            @Override
            public void subscribe(Flow.Subscriber subscriber) {
                subscriber.onSubscribe(
                        new Flow.Subscription() {
                            @Override
                            public void request(long n) {
                                subscriber.onNext("data");
                                subscriber.onComplete();
                            }

                            @Override
                            public void cancel() {

                            }
                        }
                );
            }
        }
        ;

        class Flux implements Flow.Publisher<Integer> {
            @Override
            public void subscribe(Flow.Subscriber subscriber) {
                subscriber.onSubscribe(
                        new Flow.Subscription() {
                            @Override
                            public void request(long n) {
                                for (int i = 0; i < n; i++) {
                                    subscriber.onNext("data");
                                }
                            }

                            @Override
                            public void cancel() {

                            }
                        }
                );
            }
        }
    }
}
