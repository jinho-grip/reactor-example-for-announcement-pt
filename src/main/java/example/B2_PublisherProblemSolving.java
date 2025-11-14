package example;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;

public class B2_PublisherProblemSolving {

    public static void main(String[] args) throws InterruptedException {

        var executor = Executors.newFixedThreadPool(100);

        Flow.Publisher<Integer> publisher = new Flow.Publisher<>() {
            @Override
            public void subscribe(Flow.Subscriber<? super Integer> subscriber) {

                subscriber.onSubscribe(
                        new Flow.Subscription() {

                            @Override
                            public void request(long n) {
                                for (int i = 0; i < n; i++) {
                                    int currentItem = new Random().nextInt(10000) + 1;
                                    subscriber.onNext(currentItem);
                                }
                            }

                            @Override
                            public void cancel() {
                                subscriber.onComplete();
                            }
                        }
                );
            }
        };

        class Subscriber implements Flow.Subscriber<Integer> {
            Flow.Subscription subscription;
            int available = 10;

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                this.subscription = subscription;
                subscription.request(available);
            }

            @Override
            public void onNext(Integer item) {
                executor.execute(
                        () -> {
                            try {
                                Thread.sleep(3000);
                                System.out.println("Received: " + item);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            subscription.request(1);
                        }
                );
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onComplete() {
            }
        }

        Subscriber subscriber = new Subscriber();
        publisher.subscribe(subscriber);
    }
}