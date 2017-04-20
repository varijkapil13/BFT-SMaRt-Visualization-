package bftsmart.demo.keyvalue;

import static bftsmart.demo.keyvalue.KeyVaueClient.RequestType;
import static bftsmart.demo.keyvalue.KeyVaueClient.ResponseStatus;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultRecoverable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.TreeMap;

/**
 * Example replica that implements a BFT replicated service (a counter).
 *
 */

public final class Server extends DefaultRecoverable {

	private TreeMap<String, String> store;

	ServiceReplica replica = null;

	public Server(int id) {
		replica = new ServiceReplica(id, this, this);
		store = new TreeMap<String, String>();
	}

	@Override
	public byte[][] appExecuteBatch(byte[][] commands,
			MessageContext[] msgCtxs) {

		byte[][] replies = new byte[commands.length][];
		for (int i = 0; i < commands.length; i++) {
			if (msgCtxs != null && msgCtxs[i] != null) {
				replies[i] = executeSingle(commands[i], msgCtxs[i]);
			} else
				executeSingle(commands[i], null);
		}

		return replies;
	}

	@Override
	public byte[] appExecuteUnordered(byte[] command, MessageContext msgCtx) {
		try {
			Producer producerObject = new Producer();
	    	producerObject.unorderedRequestFromClientRecieved(this.replica.getId(),msgCtx.getSender());
	    	
			ByteArrayInputStream in = new ByteArrayInputStream(command);
			int requestType = new DataInputStream(in).readInt();
			RequestType type = RequestType.values()[requestType];

			if (type != RequestType.GET) {
				ByteArrayOutputStream out = new ByteArrayOutputStream(4);
				new DataOutputStream(out)
						.writeInt(ResponseStatus.ERROR.ordinal());
				new DataOutputStream(out).writeUTF(
						"Only get operations can be executed unordered!");
				return out.toByteArray();
			}

			String key = new DataInputStream(in).readUTF();

			if (key == null || key.length() == 0) {
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				new DataOutputStream(out)
						.writeInt(ResponseStatus.ERROR.ordinal());
				new DataOutputStream(out)
						.writeUTF(String.format("Could not extract key!"));
				return out.toByteArray();
			}

			ByteArrayOutputStream out = get(key);
			return out.toByteArray();
		} catch (IOException ex) {
			System.err.println("Invalid request received!");
			return new byte[0];
		}
	}

	private ByteArrayOutputStream get(String key) throws IOException {
		String value = store.get(key);

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		new DataOutputStream(out).writeInt(ResponseStatus.GET_OK.ordinal());
		if (value != null) {
			new DataOutputStream(out).writeUTF(value);
		}
		return out;
	}

	private byte[] executeSingle(byte[] command, MessageContext msgCtx) {
		try {
			
			Producer producerObject = new Producer();
			producerObject.orderedRequestFromClientManagerReceivedAtServer(this.replica.getId(), msgCtx.getSender());
			
			ByteArrayInputStream in = new ByteArrayInputStream(command);
			int requestType = new DataInputStream(in).readInt();
			RequestType type = RequestType.values()[requestType];

			if (type == RequestType.PUT) {
				System.out.println(" --- proxy --- Received PUT request!");
				String key = new DataInputStream(in).readUTF();
				String value = new DataInputStream(in).readUTF();

				if (key == null || key.length() == 0) {
					ByteArrayOutputStream out = new ByteArrayOutputStream();
					new DataOutputStream(out)
							.writeInt(ResponseStatus.ERROR.ordinal());
					new DataOutputStream(out)
							.writeUTF(String.format("Could not extract key!"));
					return out.toByteArray();
				}

				if (value == null) {
					ByteArrayOutputStream out = new ByteArrayOutputStream();
					new DataOutputStream(out)
							.writeInt(ResponseStatus.ERROR.ordinal());
					new DataOutputStream(out).writeUTF(
							String.format("Could not extract value!"));
					return out.toByteArray();
				}

				store.put(key, value);

				ByteArrayOutputStream out = new ByteArrayOutputStream();
				new DataOutputStream(out)
						.writeInt(ResponseStatus.PUT_OK.ordinal());
				return out.toByteArray();
			} else if (type == RequestType.GET) {
				System.out.println(" --- proxy --- Received GET request!");
				String key = new DataInputStream(in).readUTF();

				if (key == null || key.length() == 0) {
					ByteArrayOutputStream out = new ByteArrayOutputStream();
					new DataOutputStream(out)
							.writeInt(ResponseStatus.ERROR.ordinal());
					new DataOutputStream(out)
							.writeUTF(String.format("Could not extract key!"));
					return out.toByteArray();
				}

				ByteArrayOutputStream out = get(key);
				return out.toByteArray();
			} else {
				ByteArrayOutputStream out = new ByteArrayOutputStream(4);
				new DataOutputStream(out)
						.writeInt(ResponseStatus.ERROR.ordinal());
				new DataOutputStream(out).writeUTF(
						"Only get operations can be executed unordered!");
				return out.toByteArray();
			}
		} catch (IOException ex) {
			System.err.println("Invalid request received!");
			return new byte[0];
		}
	}

	public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("Use: java KeyValueServer <processId>");
			System.exit(-1);
		}
		new Server(Integer.parseInt(args[0]));
	}

	@SuppressWarnings("unchecked")
	@Override
	public void installSnapshot(byte[] state) {
		try {
			System.out.println("setState called");
			ByteArrayInputStream bis = new ByteArrayInputStream(state);
			ObjectInput in = new ObjectInputStream(bis);
			TreeMap<String, String> storedMap = (TreeMap<String, String>) in
					.readObject();
			store = storedMap;
			in.close();
			bis.close();
		} catch (Exception e) {
			System.err.println(
					"[ERROR] Error deserializing state: " + e.getMessage());
		}
	}

	@Override
	public byte[] getSnapshot() {
		try {
			System.out.println("getState called");
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutput out = new ObjectOutputStream(bos);
			out.writeObject(store);
			out.flush();
			bos.flush();
			out.close();
			bos.close();
			return bos.toByteArray();
		} catch (IOException ioe) {
			System.err.println(
					"[ERROR] Error serializing state: " + ioe.getMessage());
			return "ERROR".getBytes();
		}
	}
}