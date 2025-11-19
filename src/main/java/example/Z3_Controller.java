package example;

import reactor.core.publisher.Mono;

public class Z3_Controller {
    private final F1_SimpleWebClient webClient;

    public Z3_Controller(F1_SimpleWebClient webClient) {
        this.webClient = webClient;
    }

    public Mono<String> handle(String msg) {
        Mono<String> mono = webClient.sendMessage("localhost", 8090, msg);
        return mono.map(
                result -> "Processed: " + result
        );
    }
}

