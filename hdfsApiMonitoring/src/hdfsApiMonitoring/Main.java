package hdfsApiMonitoring;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Main {

	public static void main(String[] args) {
		String postgres_host = args[0];
		String postgres_user = args[1];
		String postgres_password = args[2];
		String KNOX_URL = args[3];
		try {
			Class.forName("org.postgresql.Driver");
			String encoding = Base64.encodeBase64String("mlanciau:mlanciau-password".getBytes());
			HttpClient httpClient = HttpClientBuilder.create().build();
			String URL = "https://" + KNOX_URL + "/webhdfs/v1/tmp?op=GETCONTENTSUMMARY";
			System.out.println(URL);
			HttpGet httpGet = new HttpGet(URL);
			httpGet.addHeader("Accept", "application/json");
			httpGet.setHeader("Authorization", "Basic " + encoding);
			HttpResponse httpResponse = httpClient.execute(httpGet);
			HttpEntity responseEntity = httpResponse.getEntity();
			String responseString = EntityUtils.toString(responseEntity, "UTF-8");
			System.out.println(responseString);
			JsonNode jsonNode = mapper.readTree(responseString);
			Connection connection = DriverManager.getConnection("jdbc:postgresql://" + postgres_host + ":5432/test", postgres_user, postgres_password);
			Statement statement = connection.createStatement();
			statement.executeUpdate("CREATE TABLE IF NOT EXISTS hdfs_apps_monitoring(c_path TEXT, size BIGINT, c_session SERIAL, c_timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT current_timestamp)");
			
			// tmp directory
			
			// .staging directory
			
			// looking for small file
			
			// looking for file in .staging folder older than one week
			
			// closing
			
			statement.close();
			connection.close();
		} catch (IllegalArgumentException | IOException e) {
			System.exit(1);
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			System.exit(2);
			e.printStackTrace();
		} catch (SQLException e) {
			System.exit(3);
			e.printStackTrace();
		}
	}
	
	final static ObjectMapper mapper = new ObjectMapper();

}

/*
Configuration conf = new Configuration();
FileSystem fs = FileSystem.get(conf);
// report current directory size (focusing on the one using the 80%)
ArrayList<Path> arrayListPath = new ArrayList<>();
FileStatus[] fsStatus = fs.listStatus(new Path("/"));
for (int i = 0; i < fsStatus.length; i++) {
	arrayListPath.add(fsStatus[i].getPath());
	statement.executeUpdate("INSERT INTO hdfs_apps_monitoring VALUES('" + fsStatus[i].getPath().toString() + "', " + fs.getContentSummary(fsStatus[i].getPath()).getSpaceConsumed() + ")");
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