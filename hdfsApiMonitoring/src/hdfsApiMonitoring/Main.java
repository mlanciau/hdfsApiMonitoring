package hdfsApiMonitoring;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Main {

	public static void main(String[] args) {
		String postgres_host = args[0];
		String postgres_user = args[1];
		String postgres_password = args[2];
		Configuration conf = new Configuration();
		if (args.length > 3 && args[3] != " ") {
			conf.addResource(new Path(args[3]));
		}
		if (args.length > 4 && args[4] != " ") {
			conf.addResource(new Path(args[4]));
		}
		try {
			Class.forName("org.postgresql.Driver");
			Connection connection = DriverManager.getConnection("jdbc:postgresql://" + postgres_host + ":5432/test", postgres_user, postgres_password);
			Statement statement = connection.createStatement();
			statement.executeUpdate("CREATE TABLE IF NOT EXISTS hdfs_apps_monitoring(c_path TEXT, size BIGINT, c_session SERIAL, c_timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT current_timestamp)");
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
			
			// looking for small file
			
			// looking for file in .staging folder older than one week
			
			// closing
			fs.close();
			statement.close();
			connection.close();
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
