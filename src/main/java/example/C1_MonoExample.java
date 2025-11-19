package example;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class C1_MonoExample {

    public static void main(String[] args) {
        Mono<Integer> mono = Mono.just(4);

        mono.map(data -> data + 10)
                .map(data -> data * 2)
                .filter(data -> data > 30)
                .map(data -> data / 2)
                .subscribe(data -> System.out.println("최종 결과: " + data));

        System.out.println("==============================");

        Flux<Integer> flux = Flux.just(1, 2, 3, 4, 5);
        flux = flux.map(data -> data * 10);
        flux.subscribe(data -> System.out.println("데이터: " + data));
    }
}
