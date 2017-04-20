package bftsmart.demo.keyvalue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Scanner;

import bftsmart.tom.ServiceProxy;

/**
 * Example client that updates a BFT replicated service (a counter).
 *
 */
public class KeyVaueClient {

	public enum RequestType {
		PUT, GET;
	}

	public enum ResponseStatus {
		PUT_OK, GET_OK, ERROR;
	}

	public static void main(String[] args) throws IOException {
		if (args.length < 1) {
			System.out.println("Usage: java ...CounterClient <process id>");
			System.exit(-1);
		}

		ServiceProxy keyValueProxy = new ServiceProxy(
				Integer.parseInt(args[0]));

		Scanner scanner = null;
		try {
			scanner = new Scanner(System.in);

			System.out.println(
					" --- Get values from store or put values into it.");
			while (true) {
				System.out.println(" --- ");
				String input = scanner.nextLine();

				if (input == null) {
					System.out.println(" --- ");
				} else if ("q".equals(input) || "quit".equals(input)
						|| "exit".equals(input)) {
					System.out.println("Exit!");
					break;
				} else if (input.startsWith("get")) {
					String[] commandArgs = input.split(" ");
					if (commandArgs.length != 2) {
						System.out.println(
								" --- Get command needs one additional argument!");
						System.out.println(" --- ");
					} else {
						String key = commandArgs[1];

						System.out.println(String.format(
								"Getting the value for key %s...", key));

						processReply(executeGet(keyValueProxy, key));
					}
				} else if (input.startsWith("put")) {
					String[] commandArgs = input.split(" ");
					if (commandArgs.length != 3) {
						System.out.println(
								" --- Put command needs two additional arguments!");
						System.out.println(" --- ");
					} else {
						String key = commandArgs[1];
						String value = commandArgs[2];

						System.out.println(String.format(
								"Putting value %s to key %s.", value, key));

						processReply(executePut(keyValueProxy, key, value));
					}
				} else {
					System.out.println(" --- Did not understand your command!");
					System.out.println(" --- ");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			scanner.close();
			keyValueProxy.close();
		}
	}

	private static byte[] executeGet(ServiceProxy proxy, String key)
			throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		new DataOutputStream(out).writeInt(RequestType.GET.ordinal());
		new DataOutputStream(out).writeUTF(key);

		return proxy.invokeUnordered(out.toByteArray());
	}

	private static byte[] executePut(ServiceProxy proxy, String key,
			String value) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();

		new DataOutputStream(out).writeInt(RequestType.PUT.ordinal());
		new DataOutputStream(out).writeUTF(key);
		new DataOutputStream(out).writeUTF(value);

		return proxy.invokeOrdered(out.toByteArray());
	}

	private static void processReply(byte[] reply) throws IOException {
		if (reply != null) {
			ByteArrayInputStream in = new ByteArrayInputStream(reply);
			int responseStatus = new DataInputStream(in).readInt();
			ResponseStatus status = ResponseStatus.values()[responseStatus];

			switch (status) {
			case PUT_OK:
				System.out.println(String.format(" --- Putting succeded!"));
				break;
			case GET_OK:
				String value = "";
				try {
					value = new DataInputStream(in).readUTF();
				} catch (EOFException ignore) {
					System.out.println(ignore.getMessage());
				}
				System.out.println(String.format(" --- Value: %s", value));
				break;
			case ERROR:
				String reason = new DataInputStream(in).readUTF();
				System.out.println(String
						.format(" --- Getting response failed! %s", reason));
				break;
			default:
				System.out.println(" --- Did not understand response status!");
				System.out.println(" --- ");
				break;
			}
		} else {
			System.out.println(
					" --- Did not understand the reply from the service proxy!");
			System.out.println(" --- ");
		}
	}
}