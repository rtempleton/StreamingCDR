package com.github.rtempleton.cdr_storm;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public abstract class AbstractTestCase {
	
    private Log logger;
    

    /**
     * Gets the logger for this test.
     * 
     * @return the logger for this test
     */
    protected Log getLogger() {
        if (this.logger == null)
            this.logger = LogFactory.getLog(this.getClass().getName());
        return this.logger;
    }

    /**
     * Generates a seed for the current test.
     * <p>
     * This method is equivalent to <code>getSeed(null)</code>.
     * 
     * @return the generated seed
     */
    protected long getSeed() {
        return getSeed(null);
    }

    /**
     * Generates a seed for the current test and outputs that seed to the logger provided by {@code
     * AbstractDataRushTestCase#getLogger()}. If <code>testName</code> is null, no log message will be output.
     * 
     * @param testName
     *        the test name to include in the log message
     * @return the generated seed
     */
    protected long getSeed(String testName) {
        long seed = System.currentTimeMillis();
        if (testName != null)
            System.out.println("Seed for test " + testName + " is " + seed + "L");
        return seed;
    }
    
    
    /**
     * Gets the path to the resource directory of the given class.
     * 
     * @param testClass test class to use as basis for the resource path
     * @return the file path to the specified resource
     */
    protected static String getResourcePath(Class<?> testClass) {
        return getResourcePath(testClass, null);
    }

    /**
     * Gets the file path to a specified resource. A <code>resourceName</code> of null will return the path to the
     * directory containing the resources for this test.
     * 
     * @param resourceName
     *        the resource for which to retrieve the path
     * @return the file path to the specified resource
     */
    protected String getResourcePath(String resourceName) {
        return getResourcePath(this.getClass(), resourceName);
    }

    /**
     * Gets the file path to a specified resource for the given class. A <code>resourceName</code> of null will
     * return the path to the directory containing the resources for the given class.
     * 
     * @param resourceName
     *        the resource for which to retrieve the path
     * @return the file path to the specified resource
     */
    protected static String getResourcePath(
            Class<?> testClass,
            String resourceName) {
        return new TestHelper(testClass).getResourcePath(resourceName);
    }

    /**
     * Gets the path to the directory containing resources for this test.
     * <p>
     * This method is equivalent to <code>getResourcePath(null)</code>.
     * 
     * @return the path to the directory containing resources for this test
     */
    protected String getResourcePath() {
        return getResourcePath(this.getClass(), null);
    }


    /**
     * Gets the file path to a specified scratch file. A <code>fileName</code> of null will return the path to the
     * scratch directory for this test.
     * 
     * @param fileName
     *        the scratch file for which to get the path
     * @return the file path to the specified scratch file
     */
    protected String getScratchPath(String fileName) {
        return new TestHelper(this.getClass()).getScratchPath(fileName);
    }
    
    /**
     * Gets the path to the scratch directory for this test.
     * <p>
     * This method is equivalent to <code>getScratchPath(null)</code>.
     * 
     * @return the path to the scratch directory for this test
     */
    protected String getScratchPath() {
        return getScratchPath(null);
    }

    /**
     * Creates a scratch directory with the given name
     * beneath the test's scratch directory.
     * @param fileName the directory to create
     * @return the path to the scratch directory
     */
    protected File createScratchDirectory(String fileName) {
        File dir = new File(getScratchPath(fileName));
        if (!dir.exists() || !dir.isDirectory()) {
            if (!dir.mkdirs())
                throw new RuntimeException("Failed to create scratch directory '" + dir + "'");
        }
        return dir;


    }
    
    /**
     * Creates the scratch directory for the test
     * @return the path to the scratch directory
     */
    protected File createScratchDirectory() {
        return createScratchDirectory(null);
    }
    
   
	



}
