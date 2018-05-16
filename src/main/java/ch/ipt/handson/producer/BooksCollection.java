package ch.ipt.handson.producer;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;

import ch.ipt.handson.model.*;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class BooksCollection {

    private static final String RESOURCE_FILE = "data/books-kafka.json";

    static ArrayList<Book> books = new ArrayList<>();

    static {
        ClassLoader classLoader = BooksCollection.class.getClassLoader();
        File file = new File(classLoader.getResource(RESOURCE_FILE).getFile());

        JsonArray jsonBooksArray = null;
        try {
            jsonBooksArray = Json.parse(new FileReader(file)).asArray();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        for (JsonValue book : jsonBooksArray) {
            JsonObject bookObject = book.asObject();
            JsonObject volumeInfo = bookObject.get("volumeInfo").asObject();

            String id = bookObject.get("id").asString();
            String title = volumeInfo.get("title").asString();

            JsonValue authorsObj = volumeInfo.get("authors");
            String authors = authorsObj == null ? null : authorsObj.asArray().values().stream().map(jsonValue -> jsonValue.asString()).collect(Collectors.joining(", "));

            JsonValue categoriesObj = volumeInfo.get("categories");
            String categories = categoriesObj == null ? null : categoriesObj.asArray().values().stream().map(jsonValue -> jsonValue.asString()).collect(Collectors.joining(", "));

            Integer price = null;
            JsonObject saleInfo = bookObject.get("saleInfo").asObject();
            if (saleInfo != null) {
                JsonValue listPrice = saleInfo.asObject().get("listPrice");
                if (listPrice != null) {
                    JsonValue amount = listPrice.asObject().get("amount");
                    if (amount != null) {
                        price = Math.round(amount.asFloat() * 100F);
                    }
                }
            }

            if (price == null) {
                price = 50 * title.charAt(0);
            }

            books.add(Book.newBuilder()
                    .setBookId(id)
                    .setTitle(title)
                    .setAuthors(authors)
                    .setCategories(categories)
                    .setPrice(price)
                    .build());
        }
    }

    public static List<Book> getBooks() {
        return books;
    }
}
