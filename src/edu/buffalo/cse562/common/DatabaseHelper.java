package edu.buffalo.cse562.common;

import java.io.File;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class DatabaseHelper {

	public static void createIndexes(){
		Environment env = null;
		Database db = null;
		
		try{
			//Open the environment
			EnvironmentConfig envConfig = new EnvironmentConfig();
			envConfig.setAllowCreate(true);
			env = new Environment(new File("/export/dbEnv"), envConfig);
			//Open the db
			DatabaseConfig dbConfig = new DatabaseConfig();
			dbConfig.setAllowCreate(true);
			db = env.openDatabase(null, "sampleDatabase", dbConfig);
		}catch(DatabaseException dbe){
			dbe.printStackTrace();
		}
	}

}
