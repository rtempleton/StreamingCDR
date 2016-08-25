package com.github.rtempleton.cdr_storm;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

public final class TestHelper {

	private static final String SCRATCH_DIR;

    static {
        String scratchDir = System.getProperty("scratch.dir");
        if (scratchDir == null)
            scratchDir = "target/scratch";
        SCRATCH_DIR = new File(scratchDir).getAbsolutePath().replace('\\', '/').replaceFirst("/$", "");
        System.out.println("Scratch directory is " + SCRATCH_DIR);
    }
    
    private final Class<?> testClass;
    
    /**
     * Creates a helper for the specified test class.
     * Paths produced will be based on the fully qualified
     * path of the class.
     * 
     * @param testClass the class object of the test with
     * which the helper is associated
     */
    public TestHelper(Class<?> testClass) {
        this.testClass= testClass;
    }
    
    /**
     * Return the base scratch path.
     * 
     * @return base scratch path
     */
    public static String getBaseScratchPath() {
        return SCRATCH_DIR;
    }

    
    /**
     * Gets the file path to a specified scratch file. A <code>fileName</code> of null will return the path to the
     * scratch directory for this test.
     * 
     * @param fileName
     *        the scratch file for which to get the path
     * @return the file path to the specified scratch file
     */
    public String getScratchPath(String fileName) {
        String path = SCRATCH_DIR + '/' + testClass.getPackage().getName().replace('.', '/');
        if (fileName != null && !fileName.isEmpty())
            path += '/' + fileName.replace('\\', '/');
        return path;
    }
    
    /**
     * Gets the file path to a specified resource. A <code>resourceName</code> of null will return the path to the
     * directory containing the resources for this test.
     * 
     * @param resourceName
     *        the resource for which to retrieve the path
     * @return the file path to the specified resource
     */
    public String getResourcePath(String resourceName) {
        if (resourceName == null)
            resourceName = "";

        String resource = resourceName.isEmpty() ? testClass.getSimpleName() + ".class" : resourceName;
        String path;
        try {
            URL resourceUrl = testClass.getResource(resource);
            if (resourceUrl == null ) {
                throw new IllegalArgumentException("unable to load resource " + resourceName);
            }
            path = resourceUrl.toURI().getPath().replace('\\', '/');
        } catch (URISyntaxException urise) {
            throw new AssertionError(urise);
        }

        if (resourceName.isEmpty())
            path = path.substring(0, path.length() - (resource.length() + 1)); // remove "/Foo.class"

        return path;
    }

}
