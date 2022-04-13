package br.com.ak.ecommerce;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
	
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		try(var dispatcher = new KafkaDispatcher()) {			
			// Simulando 100 pedidos de uma vez
			for (var i=0; i < 10; i++) {			
				var key = UUID.randomUUID().toString(); // Exemplo de key aleatória
				var value = key + ",456456,999999";
				dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);
				
				var email = "Thank you for your order! We are processing your order...";
				dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
			}
		}
	}
}
