package example;

public class Z2_DependencyInjectionManager {
    private F1_SimpleWebClient webClient;
    private Z3_Controller controller;

    public void setWebClient(F1_SimpleWebClient webClient) {
        this.webClient = webClient;
    }

    public void setController(Z3_Controller controller) {
        this.controller = controller;
    }


    public F1_SimpleWebClient getWebClient() {
        return webClient;
    }

    public Z3_Controller getController() {
        return controller;
    }
}