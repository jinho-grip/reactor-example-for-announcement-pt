package example;

import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Consumer;
import java.util.function.Function;

public class C2_CustomMono {

    static class CustomMono implements Flow.Publisher<Integer> {

        int data;

        public CustomMono(int data) {
            this.data = data;
        }

        public static CustomMono just(Integer data) {
            return new CustomMono(data);
        }

        @Override
        public void subscribe(Flow.Subscriber<? super Integer> subscriber) {
            subscriber.onSubscribe(
                    new Subscription() {
                        @Override
                        public void request(long n) {
                            subscriber.onNext(data);
                            subscriber.onComplete();
                        }

                        @Override
                        public void cancel() {
                        }
                    }
            );
        }

        public void subscribe(Consumer<? super Integer> consumer) {
            this.subscribe(new Flow.Subscriber<>() {
                @Override
                public void onSubscribe(Subscription subscription) {
                    subscription.request(1);
                }

                @Override
                public void onNext(Integer item) {
                    consumer.accept(item);
                }

                @Override
                public void onError(Throwable throwable) {
                }

                @Override
                public void onComplete() {
                }
            });
        }
    }

    private static CustomMono mapPub(CustomMono publisher, Function<Integer, Integer> f) {
        return new CustomMono(f.apply(publisher.data));
    }

    public static void main(String[] args) {
        CustomMono mono = CustomMono.just(4);
        mono = mapPub(mono, data -> data * 10);
        mono.subscribe(data -> System.out.println("완료 : " + data));
    }
}
