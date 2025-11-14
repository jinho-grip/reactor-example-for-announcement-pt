package example;

import java.util.concurrent.Flow;

// 사실상 이벤트 드리븐임
public class B1_Publisher {
    public static void main(String[] args) {

        Flow.Publisher publisher = new Flow.Publisher() {
            @Override
            public void subscribe(Flow.Subscriber subscriber) {
                subscriber.onSubscribe(
                        new Flow.Subscription() {
                            @Override
                            public void request(long n) {
                                for (int i = 0; i < n; i++) {
                                    subscriber.onNext(i);
                                }
                                subscriber.onComplete();
                            }

                            @Override
                            public void cancel() {

                            }
                        }
                );
            }
        };

        Flow.Subscriber subscriber = new Flow.Subscriber<Integer>() {
            Flow.Subscription subscription;

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                this.subscription = subscription;
                subscription.request(1);
            }

            @Override
            public void onNext(Integer item) {
                System.out.println("on Next : " + item);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }

                subscription.request(1);
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onComplete() {

            }
        };

        publisher.subscribe(subscriber);
    }
}