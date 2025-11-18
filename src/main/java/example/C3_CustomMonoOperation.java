package example;

import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Consumer;
import java.util.function.Function;

public class C3_CustomMonoOperation {

    static class CustomMono implements Flow.Publisher<Integer> {

        int data; // 데이터를 직접 저장

        public CustomMono(int data) {
            this.data = data;
        }

        public static CustomMono just(Integer data) {
            return new CustomMono(data);
        }

        public CustomMono map(Function<Integer, Integer> f) {
            return new CustomMono(f.apply(this.data));
        }

        @Override
        public void subscribe(Flow.Subscriber<? super Integer> subscriber) {
            subscriber.onSubscribe(
                    new Subscription() {
                        @Override
                        public void request(long n) {
                            subscriber.onNext(data); // 저장된 데이터를 전달
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

    public static void main(String[] args) {
        CustomMono mono = CustomMono.just(4);
        mono = mono.map(data -> data * 10); // 4 -> 40
        mono.subscribe(data -> System.out.println("완료 : " + data)); // "완료 : 40" 출력
    }
}
