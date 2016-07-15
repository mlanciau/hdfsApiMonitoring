package hdfsApiMonitoring;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class Main {

	public static void main(String[] args) {
		String postgres_host = args[0];
		String postgres_user = args[1];
		String postgres_password = args[2];
		String KNOX_URL = args[3];
		String cluster_name = args[4];
		String knox_user = args[5];
		String knox_password = args[6];
		String with_deletion = args[7];
		try {
			Class.forName("org.postgresql.Driver");
			String encoding = Base64.encodeBase64String((knox_user + ":" + knox_password).getBytes());
			Connection connection = DriverManager.getConnection("jdbc:postgresql://" + postgres_host + ":5432/test", postgres_user, postgres_password);
			Statement statement = connection.createStatement();
			statement.executeUpdate("CREATE TABLE IF NOT EXISTS hdfs_apps_monitoring(c_path TEXT, "
					+ "c_spaceconsumed BIGINT, c_length BIGINT, c_directorycount INT, "
					+ "c_filecount INT, c_quota BIGINT, c_spacequota BIGINT, c_session BIGINT DEFAULT 0, c_level INT, c_cluster TEXT DEFAULT '" + cluster_name + "',"
					+ "c_timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT current_timestamp)");
			//arrayListPath.add("/");
			ResultSet resultSet = statement.executeQuery("SELECT COALESCE(MAX(c_session), 0) + 1 as c_max_session FROM hdfs_apps_monitoring");
			long c_session = 0;
			int c_level = 0;
			ArrayList<String> arrayListPath = getSubPath("", KNOX_URL, encoding, with_deletion);
			ArrayList<String> arrayListSubPath = new ArrayList<>();
			if (resultSet.next()) {
				c_session = resultSet.getLong("c_max_session");
			}
			resultSet.close();
			for (c_level = 0; c_level < 5; c_level++) {
				System.out.println("level : " + c_level);
				arrayListSubPath = new ArrayList<>();
				for (String path : arrayListPath) {
					getSpaceConsumed(path, KNOX_URL, encoding, statement, c_session, c_level);
					arrayListSubPath.addAll(getSubPath(path, KNOX_URL, encoding, with_deletion));
				}
				arrayListPath = arrayListSubPath;
			}
			
			// /tmp/hive directory
			
			// .staging directory
			
			// looking for small file
			
			// closing
			statement.close();
			connection.close();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(2);
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(3);
		}
	}
	
	private static void getSpaceConsumed(String dir, String KNOX_URL, String encoding, Statement statement, long c_session, int c_level) {
		try {
			String URL = "https://" + KNOX_URL + "/webhdfs/v1" + dir + "?op=GETCONTENTSUMMARY";
			HttpClient httpClient = HttpClientBuilder.create().build();
			HttpGet httpGet = new HttpGet(URL);
			httpGet.addHeader("Accept", "application/json");
			httpGet.setHeader("Authorization", "Basic " + encoding);
			HttpResponse httpResponse = httpClient.execute(httpGet);
			HttpEntity responseEntity = httpResponse.getEntity();
			String responseString = EntityUtils.toString(responseEntity, "UTF-8");
			if (responseString.contains("FileNotFoundException")) {
				return;
			}
			JsonNode jsonNode = mapper.readTree(responseString);
			JsonNode jsonNodeCurrent = jsonNode.at("/ContentSummary");
			if (jsonNodeCurrent != null && jsonNodeCurrent.get("spaceConsumed") != null) {
				statement.executeUpdate("INSERT INTO hdfs_apps_monitoring VALUES('" + dir + 
						"', " + jsonNodeCurrent.get("spaceConsumed").asLong()  + ", " + jsonNodeCurrent.get("length").asLong() + ", " +
						jsonNodeCurrent.get("directoryCount").asInt() + ", " + jsonNodeCurrent.get("fileCount").asInt() + ", " +
						jsonNodeCurrent.get("quota").asInt() + ", " + jsonNodeCurrent.get("spaceQuota").asInt() + ", " + c_session + ", " + c_level + 
						")");
			} else {
				System.out.println(responseString);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(5);
		}
	}
	
	private static void houseCleaning(String dir, String KNOX_URL, String encoding) {
		try {
			String URL = "https://" + KNOX_URL + "/webhdfs/v1" + dir + "?op=DELETE&recursive=true";
			HttpClient httpClient = HttpClientBuilder.create().build();
			HttpDelete httpDelete = new HttpDelete(URL);
			httpDelete.addHeader("Accept", "application/json");
			httpDelete.setHeader("Authorization", "Basic " + encoding);
			HttpResponse httpResponse = httpClient.execute(httpDelete);
			HttpEntity responseEntity = httpResponse.getEntity();
			String responseString = EntityUtils.toString(responseEntity, "UTF-8");
			System.out.println(responseString);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(6);
		}
	}
	
	private static ArrayList<String> getSubPath(String dir, String KNOX_URL, String encoding, String with_deletion) {
		ArrayList<String> arrayListPath = new ArrayList<>();
		try {
			String URL = "https://" + KNOX_URL + "/webhdfs/v1" + dir + "?op=LISTSTATUS";
			HttpClient httpClient = HttpClientBuilder.create().build();
			HttpGet httpGet = new HttpGet(URL);
			httpGet.addHeader("Accept", "application/json");
			httpGet.setHeader("Authorization", "Basic " + encoding);
			HttpResponse httpResponse = httpClient.execute(httpGet);
			HttpEntity responseEntity = httpResponse.getEntity();
			String responseString = EntityUtils.toString(responseEntity, "UTF-8");
			JsonNode jsonNode = mapper.readTree(responseString);
			if (responseString.contains("FileNotFoundException")) {
				return arrayListPath;
			}
			ArrayNode jsonArray = (ArrayNode) jsonNode.at("/FileStatuses/FileStatus");
			Iterator<JsonNode> appsIterator = jsonArray.elements();
			JsonNode jsonNodeCurrent = null;
			while (appsIterator.hasNext()) {
				jsonNodeCurrent = appsIterator.next();
				//System.out.println(jsonNodeCurrent.get("pathSuffix").asText());
				//System.out.println(jsonNodeCurrent.get("modificationTime").asLong() + " " + Calendar.getInstance().getTimeInMillis());
				if ((jsonNodeCurrent.get("pathSuffix").asText().equals("teragen") || jsonNodeCurrent.get("pathSuffix").asText().equals("terasort") || jsonNodeCurrent.get("pathSuffix").asText().equals("teravalidate") || jsonNodeCurrent.get("pathSuffix").asText().equals("TestDFSIO")
						|| dir.endsWith(".staging") || ((dir.startsWith("/tmp/hive")) && !dir.endsWith("/tmp/hive") && !jsonNodeCurrent.get("pathSuffix").asText().equals("_tez_session_dir")))) {
					if (jsonNodeCurrent.get("modificationTime").asLong() < Calendar.getInstance().getTimeInMillis() - 604800000) {
						if (with_deletion.equals("y") || with_deletion.equals("yes")) {
							System.out.println("Asking for deletion : " + dir + "/" + jsonNodeCurrent.get("pathSuffix").asText() + "   " + simpleDateFormat.format(new Date(jsonNodeCurrent.get("modificationTime").asLong())) + "   and   " + simpleDateFormat.format(new Date((Calendar.getInstance().getTimeInMillis() - 604800000))));
							houseCleaning(dir + "/" + jsonNodeCurrent.get("pathSuffix").asText(), KNOX_URL, encoding);
						} else {
							System.out.println("To check for deletion : " + dir + "/" + jsonNodeCurrent.get("pathSuffix").asText() + "   " + simpleDateFormat.format(new Date(jsonNodeCurrent.get("modificationTime").asLong())) + "   and   " + simpleDateFormat.format(new Date((Calendar.getInstance().getTimeInMillis() - 604800000))));
						}
					} else {
						System.out.println("Next time : " + dir + "/" + jsonNodeCurrent.get("pathSuffix").asText() + "   " + simpleDateFormat.format(new Date(jsonNodeCurrent.get("modificationTime").asLong())) + "   and   " + simpleDateFormat.format(new Date((Calendar.getInstance().getTimeInMillis() - 604800000))));
					}
				} else {
					arrayListPath.add(dir + "/" + jsonNodeCurrent.get("pathSuffix").asText());
				}
				
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(4);
		}
		return arrayListPath;
	}
	
	final static ObjectMapper mapper = new ObjectMapper();
	final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy MM dd HH:mm:ss");
	
}

/* with the previous version
Configuration conf = new Configuration();
FileSystem fs = FileSystem.get(conf);
// report current directory size (focusing on the one using the 80%)
ArrayList<Path> arrayListPath = new ArrayList<>();
FileStatus[] fsStatus = fs.listStatus(new Path("/"));
for (int i = 0; i < fsStatus.length; i++) {
	arrayListPath.add(fsStatus[i].getPath());
}

ArrayList<Path> arrayListSubPath = new ArrayList<>();
for (Path currentPath : arrayListPath) {
	fsStatus = fs.listStatus(currentPath);
	for (int i = 0; i < fsStatus.length; i++) {
		arrayListSubPath.add(fsStatus[i].getPath());
		statement.executeUpdate("INSERT INTO hdfs_apps_monitoring VALUES('" + fsStatus[i].getPath().toString() + "', " + fs.getContentSummary(fsStatus[i].getPath()).getSpaceConsumed() + ")");
	}
}

for (Path currentPath : arrayListSubPath) {
	fsStatus = fs.listStatus(currentPath);
	for (int i = 0; i < fsStatus.length; i++) {
		statement.executeUpdate("INSERT INTO hdfs_apps_monitoring VALUES('" + fsStatus[i].getPath().toString() + "', " + fs.getContentSummary(fsStatus[i].getPath()).getSpaceConsumed() + ")");
	}
}
fs.close();
*/