package example;

import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public class C3_CustomFlux {

    abstract static class CustomFlux implements Flow.Publisher<Integer> {

        public static CustomFlux just(Integer... data) {
            return new CustomFlux() {
                @Override
                public void subscribe(Flow.Subscriber<? super Integer> subscriber) {
                    subscriber.onSubscribe(new Subscription() {

                        @Override
                        public void request(long n) {
                            for (Integer item : data) {
                                subscriber.onNext(item);
                            }
                        }

                        @Override
                        public void cancel() {
                        }
                    });
                }
            };
        }

        public CustomFlux map(Function<Integer, Integer> mapper) {
            CustomFlux upstream = this;

            return new CustomFlux() {
                @Override
                public void subscribe(Flow.Subscriber<? super Integer> subscriber) {
                    upstream.subscribe(new Flow.Subscriber<>() {
                        @Override
                        public void onSubscribe(Subscription s) {
                            subscriber.onSubscribe(s);
                        }

                        @Override
                        public void onNext(Integer item) {
                            subscriber.onNext(mapper.apply(item));
                        }

                        @Override
                        public void onError(Throwable t) {
                            subscriber.onError(t);
                        }

                        @Override
                        public void onComplete() {
                            subscriber.onComplete();
                        }
                    });
                }
            };
        }

        public CustomFlux filter(Predicate<Integer> predicate) {
            CustomFlux upstream = this;

            return new CustomFlux() {
                @Override
                public void subscribe(Flow.Subscriber<? super Integer> subscriber) {
                    upstream.subscribe(new Flow.Subscriber<>() {
                        @Override
                        public void onSubscribe(Subscription s) {
                            subscriber.onSubscribe(s);
                        }

                        @Override
                        public void onNext(Integer item) {
                            if (predicate.test(item)) subscriber.onNext(item);
                        }

                        @Override
                        public void onError(Throwable t) {
                            subscriber.onError(t);
                        }

                        @Override
                        public void onComplete() {
                            subscriber.onComplete();
                        }
                    });
                }
            };
        }

        public CustomFlux peek(Consumer<Integer> consumer) {
            CustomFlux upstream = this;

            return new CustomFlux() {
                @Override
                public void subscribe(Flow.Subscriber<? super Integer> subscriber) {
                    upstream.subscribe(new Flow.Subscriber<>() {
                        @Override
                        public void onSubscribe(Subscription s) {
                            subscriber.onSubscribe(s);
                        }

                        @Override
                        public void onNext(Integer item) {
                            consumer.accept(item);
                            subscriber.onNext(item);
                        }

                        @Override
                        public void onError(Throwable t) {
                            subscriber.onError(t);
                        }

                        @Override
                        public void onComplete() {
                            subscriber.onComplete();
                        }
                    });
                }
            };
        }

        public void subscribe(Consumer<? super Integer> consumer) {
            this.subscribe(new Flow.Subscriber<>() {
                public void onSubscribe(Subscription s) {
                    s.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(Integer item) {
                    consumer.accept(item);
                }

                @Override
                public void onError(Throwable t) {
                }

                @Override
                public void onComplete() {
                }
            });
        }
    }

    public static void main(String[] args) {
        CustomFlux.just(1, 2, 3, 4, 5)
                .peek(data -> System.out.println("원본: " + data))
                .filter(data -> data % 2 == 0)
                .peek(data -> System.out.println("  -> 필터 통과: " + data))
                .map(data -> data * 10)
                .subscribe(data -> System.out.println("    => 최종: " + data));
    }
}