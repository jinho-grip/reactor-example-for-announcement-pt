package example;

import java.util.LinkedList;
import java.util.Observable;
import java.util.Observer;
import java.util.Queue;

class DataProducer extends Observable { // Subject
    public void run() {
        for (int i = 0; i < 10000; i++) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
            }
            setChanged();
            notifyObservers(i);
        }
    }
}

class DataConsumerServer implements Observer {
    Queue queue = new LinkedList<Integer>();

    @Override
    public void update(Observable o, Object arg) {
        if (queue.size() > 500) {
            throw new RuntimeException("와 서버가 터저버렸어요!");
        }

        queue.add((Integer) arg);
    }

    public void consume() {
        Object data = queue.poll();
        System.out.println(data);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {

        }
    }
}

public class A3_ObserverProblemExample {
    public static void main(String[] args) throws InterruptedException {
        DataProducer producer = new DataProducer();
        DataConsumerServer consumer = new DataConsumerServer();

        producer.addObserver(consumer);

        Thread producerThread = new Thread(() -> {
            producer.run();
        });

        Thread consumerThread = new Thread(() -> {
            while (true) {
                consumer.consume();
            }
        });

        producerThread.start();
        consumerThread.start();

        producerThread.join();
        consumerThread.join();
    }
}
