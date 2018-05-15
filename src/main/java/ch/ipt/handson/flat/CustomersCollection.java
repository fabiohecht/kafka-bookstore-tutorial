package ch.ipt.handson.flat;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CustomersCollection {

    private static final String RESOURCE_FILE = "data/customers.json";

    static ArrayList<Customer> customers = new ArrayList<>();

    static {
        ClassLoader classLoader = CustomersCollection.class.getClassLoader();
        File file = new File(classLoader.getResource(RESOURCE_FILE).getFile());

        JsonArray jsonCustomersArray = null;
        try {
            jsonCustomersArray = Json.parse(new FileReader(file)).asArray();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        for (JsonValue customer : jsonCustomersArray) {
            JsonObject customerObject = customer.asObject();

            customers.add(Customer.newBuilder()
                    .setFirstname(customerObject.get("first_name").asString())
                    .setLastname(customerObject.get("last_name").asString())
                    .setCustomerId(customerObject.get("id").asInt())
                    .setEmail(customerObject.get("email").asString())
                    .setStreet(customerObject.get("street").asString())
                    .setNumber(customerObject.get("number").asString())
                    .setZip(customerObject.get("zip").asString())
                    .setCity(customerObject.get("city").asString())
                    .setCountry(customerObject.get("country").asString())
                    .build());
        }
    }

    public static List<Customer> getCustomers() {
        return customers;
    }
}
