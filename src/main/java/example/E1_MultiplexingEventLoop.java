package example;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Set;

public class E1_MultiplexingEventLoop {
    private Selector selector;
    boolean running = true;

    public void run() throws IOException {
        try {
            this.selector = Selector.open();
        } catch (IOException e) {
        }

        while (running) {
            try {
                selector.select();

                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> it = selectedKeys.iterator();

                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    it.remove();

                    if (key.isReadable()) {
                        // Read 요청이 들어왔을 때 어떤 작업을 이어서 할지 작성
                    } else if (key.isConnectable()) {
                        // Connect 요청이 들어왔을 때 어떤 작업을 이어서 할지 작성
                    }
                }
            } catch (IOException e) {
                throw e;
            }
        }
    }
}
