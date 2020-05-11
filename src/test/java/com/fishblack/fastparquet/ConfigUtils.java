package com.fishblack.fastparquet;

import org.junit.Assert;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Useful routines for dealing with test configuration.
 */
public class ConfigUtils {
	private static final Logger logger = Logger.getLogger("com.fishblack.fastparquet.test", null);
	
	private File resourceDir;

	/**
	 * Enum used for file search directions.
	 */
	private enum Direction {UPWARDS, DOWNWARDS};

	/**
	 * This single instance of the config utils class.
	 */
	private static final ConfigUtils INSTANCE = new ConfigUtils();

	/**
	 * Default constructor.
	 */
	private ConfigUtils() {
		findResourceDir();
	}

	public static ConfigUtils getInstance() {
		return INSTANCE;
	}

	/**
	 * Locates the resource directory under the test directory.
	 */
	private void findResourceDir() {
		this.resourceDir = null;

		File dir = findFile(new File("").getAbsoluteFile(), "test", Direction.DOWNWARDS);
		if (dir == null) {
			logger.log(Level.SEVERE, "Could not find test directory");
			return;
		}
		dir = new File(dir, "resources");

		if (!dir.exists()) {
			logger.log(Level.SEVERE, "Could not find resources directory");
			return;
		}

		this.resourceDir = dir;
		logger.log(Level.INFO, "Resource directory is {0}", this.resourceDir.getAbsolutePath());
	}

	public File getResourceDir() {
		Assert.assertNotNull("Could not find resource directory", this.resourceDir);
		return this.resourceDir;
	}

	/**
	 * Finds a file or directory relative to a starting directory either upwards (in parent directories)
	 * or downwards (in child directories). Also checks if targetName is same as starting directory
	 * @param dir
	 * @param targetName
	 * @param direction
	 * @return
	 */
	private static File findFile(File dir, String targetName, Direction direction) {
		if(dir.getName().equals(targetName))
			return dir;
		File result = null;

		File[] dirContents = dir.listFiles();
		if (dirContents != null) {
			for (File thisFile : dirContents) {
				if (thisFile.getName().equals(targetName)) {
					return thisFile;
				}
			}

			if (direction == Direction.DOWNWARDS) {
				for (File thisFile : dirContents) {
					if (thisFile.isDirectory()) {
						result = findFile(thisFile, targetName, Direction.DOWNWARDS);
						if (result != null) {
							break;
						}
					}
				}
			} else {
				result = findFile(dir.getParentFile(), targetName, Direction.UPWARDS);
			}
		}

		return result;
	}
	
	/**
	 * Loads a properties file.
	 * @param file
	 * @return
	 */
	private Properties loadPropertiesFile(File file) {
		if (!file.exists()) {
			logger.log(Level.SEVERE, "Could not find properties file " + file);
			return null;
		}

		Properties properties = new Properties();

		FileInputStream fis = null;

		try {
			fis = new FileInputStream(file);
			properties.load(fis);
			return properties;
		} catch (FileNotFoundException e) {
			logger.log(Level.SEVERE, "Could not load properties file " + file, e);
			return null;
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Could not load properties file " + file, e);
			return null;
		} finally {
			try {
				fis.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
