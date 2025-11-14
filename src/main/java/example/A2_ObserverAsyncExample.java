package example;

import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class A2_ObserverAsyncExample {

    static class IntObservable extends Observable implements Runnable {
        public void run() {
            for (int i = 0; i < 5; i++) {
                setChanged();
                notifyObservers(i);
            }
        }
    }

    static class AsyncObserver implements Observer {
        private ExecutorService executor;

        public AsyncObserver(ExecutorService executor) {
            this.executor = executor;
        }

        @Override
        public void update(Observable o, Object arg) {
            executor.submit(() -> {
                try {
                    Thread.sleep(10000);
                    System.out.println("Observer notified with argument: " + arg);
                } catch (InterruptedException e) {
                }
            });
        }
    }

    static class AsyncObserver2 implements Observer {
        private ExecutorService executor;

        public AsyncObserver2(ExecutorService executor) {
            this.executor = executor;
        }

        @Override
        public void update(Observable o, Object arg) {
            executor.submit(() -> {
                try {
                    Thread.sleep(1000);
                    System.out.println("Observer2 notified with argument: " + arg);
                } catch (InterruptedException e) {
                }
            });
        }
    }


    public static void main(String[] args) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(1);
        Observer observer1 = new AsyncObserver(executor);

        ExecutorService executor2 = Executors.newFixedThreadPool(1);
        Observer observer2 = new AsyncObserver2(executor2);

        IntObservable io = new IntObservable();
        io.addObserver(observer1);
        io.addObserver(observer2);

        io.run();

        Thread.sleep(500);
        System.out.println("Main는 계속 작업 이어 나갑니다. 이거를 보면 grip-api AsyncManager와 비슷한 동작 흐름을 만들어냈다는걸 아실 수 있습니다.");

        executor.awaitTermination(100, TimeUnit.SECONDS);
    }
}
