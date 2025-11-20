package example;

import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

public class Z4_MonoSinkExample {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("[Main] 프로그램 시작");

        // 1. 성공 케이스 (짝수 입력)
        System.out.println("\n--- Case 1: 정상 처리 요청 (입력값: 10) ---");
        startAsyncWork(10).subscribe(
                result -> System.out.println("[Subscriber] 성공 결과 받음: " + result), // onNext 처리
                error -> System.err.println("[Subscriber] 에러 발생: " + error)          // onError 처리
        );

        // 2. 실패 케이스 (홀수 입력)
        System.out.println("\n--- Case 2: 에러 발생 요청 (입력값: 11) ---");
        startAsyncWork(11).subscribe(
                result -> System.out.println("[Subscriber] 성공 결과 받음: " + result),
                error -> System.err.println("[Subscriber] 에러 처리함: " + error.getMessage())
        );

        // 비동기 처리가 끝날 때까지 메인 스레드 대기 (안 그러면 프로그램이 바로 종료됨)
        System.out.println("\n[Main] 비동기 작업 대기 중...");
        Thread.sleep(1500);
        System.out.println("[Main] 프로그램 종료");
    }

    // Mono.create와 MonoSink를 이용해 비동기 작업을 감싸는 메서드
    public static Mono<String> startAsyncWork(int value) {
        // Mono.create: 누군가 구독(subscribe)하면 이 안의 로직이 실행됨
        return Mono.create((MonoSink<String> sink) -> {

            System.out.println("[Sink] 비동기 작업 스레드 시작 준비...");

            // 별도의 스레드(비동기)에서 작업을 수행한다고 가정
            new Thread(() -> {
                try {
                    Thread.sleep(500); // 0.5초 걸리는 작업 시뮬레이션

                    if (value % 2 == 0) {
                        // 짝수면 성공 -> success 호출
                        // 이것이 구독자의 onNext -> onComplete를 트리거함
                        String message = "작업 완료 (값: " + value + ")";
                        System.out.println("[Sink] 작업 성공! success() 호출");
                        sink.success(message);
                    } else {
                        // 홀수면 실패 -> error 호출
                        // 이것이 구독자의 onError를 트리거함
                        System.out.println("[Sink] 작업 실패! error() 호출");
                        sink.error(new RuntimeException("홀수는 처리할 수 없습니다!"));
                    }

                } catch (InterruptedException e) {
                    sink.error(e);
                }
            }).start();
        });
    }
}