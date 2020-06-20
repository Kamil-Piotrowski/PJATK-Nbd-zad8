import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;

import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

public class Main {

    public static void main(String[] str) throws UnknownHostException, ExecutionException, InterruptedException {
        RiakClient client = RiakClient.newClient("127.0.0.1");
        Namespace ns = new Namespace("default","my_bucket");
        String key = "my_key";
        String value = "my_value";

        save(client, ns, key, value);
        System.out.println("fetched: "+load(client, ns, key));
        value = "modified";
        save(client, ns, key, value);
        System.out.println("fetched: "+load(client, ns, key));
        delete(client, ns, key);
        System.out.println("fetched: "+load(client, ns, key));
        client.shutdown();
    }

    private static void delete(RiakClient client, Namespace ns, String key) throws ExecutionException, InterruptedException {
        Location location = new Location(ns, key);
        DeleteValue delete = new DeleteValue.Builder(location).build();
        client.execute(delete);
    }

    private static String load(RiakClient client, Namespace ns2, String key) throws ExecutionException, InterruptedException {
        try{
            Location location2 = new Location(ns2, key);
            FetchValue fv = new FetchValue.Builder(location2).build();
            FetchValue.Response response = client.execute(fv);
            RiakObject obj = response.getValue(RiakObject.class);
            return obj.getValue().toString();
        }catch(NullPointerException e){
            return "value does not exist";
        }
    }

    private static void save(RiakClient client, Namespace ns, String key, String value) throws ExecutionException, InterruptedException {
        Location location = new Location(ns, key);
        RiakObject riakObject = new RiakObject();
        riakObject.setValue(BinaryValue.create(value));
        StoreValue store = new StoreValue.Builder(riakObject)
                .withLocation(location)
                .withOption(StoreValue.Option.W, new Quorum(3)).build();
        client.execute(store);
    }
}
