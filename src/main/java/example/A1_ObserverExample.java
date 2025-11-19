package example;

import java.util.Observable;
import java.util.Observer;

// 핵심: "데이터가 준비되면 나에게 알려줘 (Push)"라는 아이디어.
public class A1_ObserverExample {

    static class IntObservable extends Observable implements Runnable {
        public void run() {
            for (int i = 0; i < 5; i++) {
                setChanged();
                // !! 중요 부분 !!
                notifyObservers(i);
            }
        }
    }

    public static void main(String[] args) {

        Observer observer = new Observer() {
            @Override
            public void update(java.util.Observable o, Object arg) {
                System.out.println("Observer notified with argument: " + arg);
            }
        };

        Observer observer2 = new Observer() {
            @Override
            public void update(java.util.Observable o, Object arg) {
                System.out.println("Observer 2 notified with argument: " + arg);
            }
        };

        IntObservable io = new IntObservable();
        io.addObserver(observer);
        io.addObserver(observer2);
        io.run();
    }
}
