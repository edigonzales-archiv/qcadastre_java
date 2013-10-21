package org.catais.qcadastre;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.catais.qcadastre.interlis.IliReader;
import org.catais.qcadastre.utils.QGISUtils;
import org.catais.qcadastre.utils.Utils;

import ch.interlis.ili2c.Ili2cException;

/**
 * Hello world!
 *
 */
public class App 
{
	private static Logger logger = Logger.getLogger(App.class);

    public static void main( String[] args )
    {
    	logger.setLevel(Level.DEBUG);

    	String iniFileName = null;
    	
    	try {
    		// Copy log4j properties into a temporary directory.
            final Path tempDir = Files.createTempDirectory("qcadastre" + Math.random());
			InputStream is =  App.class.getResourceAsStream("log4j.properties");
			Path log4j = Paths.get((new File(tempDir.toFile(), "log4j.properties")).getAbsolutePath());
			Files.copy(is, log4j);

			// Configure log4j
			PropertyConfigurator.configure(log4j.toFile().getAbsolutePath());
			
			// Start logging
			logger.info("Start: "+ new Date());
		
			// Read properties file with all parameters.
			iniFileName = (String) args[0];
			logger.debug("Properties filename: " + iniFileName);
			
			HashMap params = Utils.readProperties(iniFileName);
			logger.debug(params);
						
			boolean doVacuum = (Boolean) params.get("vacuum");
			boolean doReindex = (Boolean) params.get("reindex");
			boolean doQgisFiles = (Boolean) params.get("qgisFiles");
			
			logger.info("doVacuum: " + doVacuum);
			logger.info("doReindex: " + doReindex);
			logger.info("doQgisFiles: " + doQgisFiles);
			
			// Create json file for QGIS
			if (doQgisFiles == true) {
				String importModelName = (String) params.get("importModelName");
				String dbschema = (String) params.get("dbschema");
								
				QGISUtils.createTopicsTableJson(importModelName, dbschema);
			}
			
			// Do the import
			{
				IliReader reader = new IliReader( itf, "21781", params );
				reader.compileModel();

				
				
			}

			
			
			

			
    	} catch (FileNotFoundException e) {
    		e.printStackTrace();
    		logger.error(e.getMessage());
    	} catch (IOException e) {
    		e.printStackTrace();
    		logger.error(e.getMessage());
    	} catch (ArrayIndexOutOfBoundsException e) {
    		e.printStackTrace();
    		logger.error(e.getMessage());
    	} catch (IllegalArgumentException e) {
    		e.printStackTrace();
    		logger.error(e.getMessage());
    	} catch (Ili2cException e) {
    		e.printStackTrace();
    		logger.error(e.getMessage());
    	}
    	
    	finally {
			// Stop logging
        	logger.info("End: "+ new Date());
		}
 

    	
   


    	
    	
    	
    	
    	
    	
        System.out.println( "Hello Stefan!" );
    }
}
