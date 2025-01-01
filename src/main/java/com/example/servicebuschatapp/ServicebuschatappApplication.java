package com.example.servicebuschatapp;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusProcessorClient;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClient;
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClientBuilder;
import com.azure.messaging.servicebus.administration.models.CreateSubscriptionOptions;
import com.azure.messaging.servicebus.administration.models.SubscriptionProperties;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import com.azure.spring.cloud.service.implementation.servicebus.factory.ServiceBusSenderClientBuilderFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnNotWebApplication;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;

import java.time.Duration;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;

@SpringBootApplication
@EnableAsync
@ConditionalOnNotWebApplication
public class ServicebuschatappApplication implements CommandLineRunner {

	@Value("${servicebus.connectionString}")
	private String connectionString;
	@Value("${servicebus.chatTopic}")
	private String topic;

	private final ServiceBusSenderClient senderClient;

	public ServicebuschatappApplication(ServiceBusSenderClient senderClient) {
		this.senderClient = senderClient;
	}

	public static void main(String[] args) {
		SpringApplication.run(ServicebuschatappApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		Scanner scanner = new Scanner(System.in);
		System.out.print("Enter name : ");
		String userHandle = scanner.nextLine();
		// Create a administrator client using connection string.
		ServiceBusAdministrationClient adminClient = new ServiceBusAdministrationClientBuilder()
				.connectionString(connectionString)
				.buildClient();
		if(!adminClient.getTopicExists(topic)){
			adminClient.createTopic(topic);
		}

		if(!adminClient.getSubscriptionExists(topic, userHandle)){
			CreateSubscriptionOptions createSubscriptionOptions = new CreateSubscriptionOptions();
			createSubscriptionOptions.setAutoDeleteOnIdle(Duration.ofMinutes(5));
//			SubscriptionProperties subProperties =
			adminClient.createSubscription(topic, userHandle, createSubscriptionOptions);

		}

		ServiceBusSenderClient serviceBusClient =  new ServiceBusClientBuilder()
														.connectionString(connectionString)
														.sender()
														.topicName(topic)
														.buildClient();

		ServiceBusProcessorClient  serviceBusProcessorClient = new ServiceBusClientBuilder()
				.connectionString(connectionString)
				.processor()
				.topicName(topic)
				.subscriptionName(userHandle)
//				.receiveMode(ServiceBusReceiveMode.PEEK_LOCK)
				.processMessage(context -> {
					System.out.printf("%s%n", context.getMessage().getBody().toString());
					context.complete();
				})
				.processError(context -> {
					System.err.printf("Error occurred: %s%n", context.getException());
				})
				.buildProcessorClient();
		serviceBusProcessorClient.start();

		ServiceBusMessage welcomeMessage = new ServiceBusMessage(String.format("%s has joined the chat",userHandle));
		serviceBusClient.sendMessage(welcomeMessage);

		String response = "";
		boolean running = true;
		while(running){
			response = scanner.nextLine();

			if(response.equalsIgnoreCase("exit")){
				System.out.println("Bye");
				running= false;
			} else {
				ServiceBusMessage message = new ServiceBusMessage(String.format("%s > %s",userHandle, response));

				serviceBusClient.sendMessage(message);
			}
		}
		serviceBusProcessorClient.stop();
		serviceBusClient.close();

	}

}
