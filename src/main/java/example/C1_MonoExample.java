package example;

import reactor.core.publisher.Mono;

public class C1_MonoExample {

    public static void main(String[] args) {
        Mono<Integer> mono = Mono.just(4);
        mono = mono.map(data -> data * 10);
        mono.subscribe(data -> System.out.println("완료: " + data));
    }
}
