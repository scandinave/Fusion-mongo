package info.scandi.fusion.database.mongo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Named;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonString;
import javax.json.JsonValue;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import info.scandi.fusion.core.ConfigurationManager;
import info.scandi.fusion.database.worker.IWorker;
import info.scandi.fusion.exception.ConfigurationException;
import info.scandi.fusion.exception.FusionException;
import info.scandi.fusion.exception.UtilitaireException;

@Named
@ApplicationScoped
public class AbstractMongoWorker implements IWorker {

	private String dataSet;
	private String communDir;
	private String distinctDir;
	/**
	 * The connection to a mongo db database.
	 */
	private MongoDatabase db;
	private MongoClient mongoClient;

	@Inject
	protected ConfigurationManager conf;
	@Inject
	protected Logger LOGGER;

	@Override
	public void init() throws ConfigurationException, FusionException {
		initCommunDir();
		initDistinctDir();
		MongoCredential credential = MongoCredential.createCredential(conf.getDatabase().getUsername(),
				conf.getDatabase().getName(), conf.getDatabase().getPassword().toCharArray());
		this.mongoClient = new MongoClient(new ServerAddress(conf.getDatabase().getHost()), Arrays.asList(credential));
		this.db = mongoClient.getDatabase(conf.getDatabase().getName());
	}

	/**
	 * Get the json file that contains data used in all tests.
	 * 
	 * @throws FusionException
	 * @throws UtilitaireException
	 */
	public void initCommunDir() throws FusionException {
		dataSet = conf.getCommon().getRootPath() + "dataset";
		communDir = dataSet.concat("/commun");
	}

	/**
	 * Méthode getCommunDir.
	 * 
	 * @throws FusionException
	 * @throws UtilitaireException
	 */
	public void initDistinctDir() throws FusionException {
		dataSet = conf.getCommon().getRootPath() + "dataset";
		distinctDir = dataSet.concat("/distinct");
	}

	@Override
	public void start() throws FusionException {
		try {
			LOGGER.info("Starting Fusion...");
			LOGGER.info("Preparation of the database");
			if (conf.getDatabase().getInit().isEnabled()) {
				this.reset();
			} else {
				this.clean();
			}
			LOGGER.info("Inserting test data...");
			this.load(this.communDir);
		} catch (Exception e1) {
			throw new FusionException("Can't start the worker", e1);
		}
	}

	@Override
	public void stop() throws FusionException {
		this.mongoClient.close();

	}

	@Override
	public void clean() throws FusionException {
		MongoCursor<String> it = this.db.listCollectionNames().iterator();
		String name;
		while (it.hasNext()) {
			name = it.next();
			MongoCollection<Document> collection = this.db.getCollection(name);
			collection.deleteMany(null);
		}
		;

	}

	@Override
	public void reset() throws FusionException {
		this.clean();
		this.load(this.conf.getInitFile());

	}

	@Override
	public void insert(String path) throws FusionException {
		this.load(path);
	}

	@Override
	public void delete(String path) throws FusionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void load(String filePath) throws FusionException {
		this.load(new File(filePath));

	}

	@Override
	public void load(File file) throws FusionException {
		try {
			JsonReader jsonReader = Json.createReader(new FileInputStream(file));
			JsonArray collections = jsonReader.readArray();
			collections.forEach(collection -> {
				JsonObject colletionJson = collection.asJsonObject();
				JsonString collectionName = colletionJson.getJsonString("collection_name");
				MongoCollection<Document> mongoCollection = this.db.getCollection(collectionName.getString());
				JsonArray documentsJson = colletionJson.getJsonArray("documents");
				List<Document> documents = new ArrayList<Document>();
				documentsJson.forEach(document -> {
					JsonObject documentJson = document.asJsonObject();
					Iterator<Entry<String, JsonValue>> it = documentJson.entrySet().iterator();
					Entry<String, JsonValue> entry = it.next();
					Document mongoDoc = new Document(entry.getKey(), entry.getValue());
					while (it.hasNext()) {
						entry = it.next();
						mongoDoc.append(entry.getKey(), entry.getValue());
					}
					documents.add(mongoDoc);
				});
				mongoCollection.insertMany(documents);
			});
		} catch (FileNotFoundException e) {
			throw new FusionException("File not found", e);
		}
	}

	@Override
	public void save() throws FusionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void restore() throws FusionException {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, String> getAllScenarii() {
		// TODO Auto-generated method stub
		return null;
	}

}
