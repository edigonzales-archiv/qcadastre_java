package org.catais.qcadastre.utils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ReadProperties {
	private static Logger logger = Logger.getLogger(ReadProperties.class);
	
	String fileName = null;
	
    public ReadProperties( String fileName ) {
		logger.setLevel(Level.DEBUG);
		
    	this.fileName = fileName;
    }
    
    public HashMap read() throws FileNotFoundException, IOException {    	
		Properties properties = new Properties();
		BufferedInputStream stream = new BufferedInputStream(new FileInputStream(fileName));
		properties.load(stream);
		stream.close();

    	HashMap params = new HashMap();

		// Database parameters
    	String host = properties.getProperty("dbhost");
    	if (host != null) {
        	params.put("dbhost", host.trim());
    	} else {
			throw new IllegalArgumentException("'dbhost' parameter not set.");
		}
		logger.debug("dbhost: " + host);
	
    	String port = properties.getProperty("dbport");
    	if (port != null) {
        	params.put("dbport", port.trim());
    	} else {
			throw new IllegalArgumentException("'dbport' parameter not set.");
		}
		logger.debug("dbport: " + port);		
		
    	String dbname = properties.getProperty("dbname");
    	if (dbname != null) {
        	params.put("dbname", dbname.trim());
    	} else {
			throw new IllegalArgumentException("'dbname' parameter not set.");
		}
		logger.debug("dbname: " + dbname);		
		
    	String schema = properties.getProperty("dbschema");
    	if (schema != null) {
        	params.put("dbschema", schema.trim());
    	} else {
			throw new IllegalArgumentException("'dbschema' parameter not set.");
		}
		logger.debug("dbschema: " + schema);		
		
    	String user = properties.getProperty("dbuser");
    	if (user != null) {
        	params.put("dbuser", user.trim());
    	} else {
			throw new IllegalArgumentException("'dbuser' parameter not set.");
		}
    	logger.debug("dbuser: " + user);		
		
    	String pwd = properties.getProperty("dbpwd");
    	if (pwd != null) {
        	params.put("dbpwd", pwd.trim());
    	} else {
			throw new IllegalArgumentException("'dbpwd' parameter not set.");
		} 
    	logger.debug("dbpwd: " + pwd);		

    	String admin = properties.getProperty("dbadmin");
    	if (admin != null) {
        	params.put("dbadmin", admin.trim());
    	} else {
			throw new IllegalArgumentException("'dbadmin' parameter not set.");
		}
    	logger.debug("dbadmin: " + admin);		
		
    	String adminpwd = properties.getProperty("dbadminpwd");
    	if (adminpwd != null) {
        	params.put("dbadminpwd", adminpwd.trim());
    	} else {
			throw new IllegalArgumentException("'dbadminpwd' parameter not set.");
		}
    	logger.debug("dbadminpwd: " + adminpwd);	
    	
    	// sqlite file
    	String sqlite = properties.getProperty("sqlite");
    	if (sqlite != null) {
    		params.put("sqlite", sqlite.trim());
    	} else {
			throw new IllegalArgumentException("'sqlite' parameter not set.");
		}
    	
    	// destination directory
    	String dstdir = properties.getProperty("dstdir");
    	if (dstdir != null) {
    		params.put("dstdir", dstdir.trim());
    	} else {
			throw new IllegalArgumentException("'dstdir' parameter not set.");
		}
    	
        // temporary destination directory
        String tmpdstdir = properties.getProperty("tmpdstdir");
        if (dstdir != null) {
            params.put("tmpdstdir", tmpdstdir.trim());
        } else {
            throw new IllegalArgumentException("'tmpdstdir' parameter not set.");
        }    	
    	
    	return params;
    }

}



