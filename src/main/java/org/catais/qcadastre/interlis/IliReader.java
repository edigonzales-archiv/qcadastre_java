package org.catais.qcadastre.interlis;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.Map;

import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.data.DataStore;
import org.geotools.data.postgis.PostgisNGDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.DataUtilities;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureSource;
import org.geotools.data.FeatureStore;
import org.geotools.data.Transaction;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.*;

import ch.interlis.ili2c.Ili2c;
import ch.interlis.ili2c.Ili2cException;
import ch.interlis.ili2c.config.Configuration;
import ch.interlis.ili2c.metamodel.*;
import ch.interlis.ilirepository.IliManager;
import ch.interlis.iom.*;
import ch.interlis.iox.*;
import ch.interlis.iom_j.itf.ItfReader;
import ch.interlis.iom_j.itf.EnumCodeMapper;
import ch.interlis.iox_j.jts.Iox2jts;
import ch.interlis.iox_j.jts.Iox2jtsException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.catais.qcadastre.utils.PGUtils;
import org.catais.qcadastre.utils.GTUtils;
import org.catais.qcadastre.utils.IliUtils;
import org.catais.qcadastre.utils.Iox2wkt;
import org.catais.qcadastre.utils.SurfaceAreaBuilder;


public class IliReader 
{
	private static Logger logger = Logger.getLogger(IliReader.class);
	
	private ch.interlis.ili2c.metamodel.TransferDescription iliTd = null;
	private HashMap tag2class = null; 
	private IoxReader ioxReader = null;
	private EnumCodeMapper enumCodeMapper = new EnumCodeMapper();
	private String modelName = null;

    private Boolean isAreaHelper = false;
    private Boolean isAreaMain = false;
    private String areaMainFeatureName = null;
    private Boolean isSurfaceHelper = false;
    private String surfaceMainFeatureName = null;
    private Boolean isSurfaceMain = false;
    private String geomName = null;
    private int tableNumber = 0;
    private String prefix = null;
	
	private HashMap params = null;

    private String dbhost = null;
    private String dbport = null;
    private String dbname = null;
    private String dbschema = null;
    private String dbuser = null;
    private String dbpwd = null;
    private String dbadmin = null;
    private String dbadminpwd = null;
    private String epsg = null;
    private String importModelName = null;
	private String importItfFile = null;
	
	private boolean enumerationText = false;
	private boolean renumberTid = false;
	
    private LinkedHashMap featureTypes = null;
	private String featureName = null;

	private ArrayList features = null;
	private ArrayList areaHelperFeatures = new ArrayList();
	private ArrayList surfaceMainFeatures = new ArrayList();
	
	private DataStore datastore = null;

    private Transaction t = null;
    

    public IliReader(HashMap params) throws IllegalArgumentException, IOException, Ili2cException, ClassNotFoundException, SQLException {
    	logger.setLevel(Level.INFO);

    	// get parameters
    	this.params = params;
    	readParams();   
    	        
        // compile interlis model
        iliTd = IliUtils.compileModel(importModelName);
        
        
        // temporary!!!!!!!!!!
        deletePostgresSchemaAndTables();
        
        
        // create database schema
        createPostgresSchemaAndTables();
        
        // get all feature types
        featureTypes = GTUtils.getFeatureTypesFromItfTransferViewables(iliTd, epsg, enumerationText);        
        
    	tag2class = ch.interlis.iom_j.itf.ModelUtilities.getTagMap(iliTd);
    }
    
    // Ueberfluessig?
    public void setEpsg(String epsg) {
    	this.epsg = epsg;
    }
        
    public void read() throws IoxException, IOException 
    {    	    	
    	logger.debug("Starting Transaction...");
    	t = new DefaultTransaction();
    	
    	ioxReader = new ch.interlis.iom_j.itf.ItfReader(new java.io.File(importItfFile));
    	((ItfReader) ioxReader).setModel(iliTd);
    	((ItfReader) ioxReader).setRenumberTids(renumberTid);
    	((ItfReader) ioxReader).setReadEnumValAsItfCode(enumerationText);
    	
    	IoxEvent event = ioxReader.read();
    	while (event != null) {	
    		if(event instanceof StartBasketEvent) {
    			StartBasketEvent basket = (StartBasketEvent) event;
    			logger.info( basket.getType() + "(oid " + basket.getBid() + ")..." );

    		} 
    		else if (event instanceof ObjectEvent) {
    			IomObject iomObj = ((ObjectEvent)event).getIomObject();
    			String tag = iomObj.getobjecttag();
    			
    			//logger.debug("tag: " + tag);
    			readObject(iomObj, tag);
    			featureName = tag;	
    		} else if (event instanceof EndBasketEvent) {
    			// do nothing
    		} else if (event instanceof EndTransferEvent) {
    			ioxReader.close();

    			// write the last table to the database		
    			this.writeToPostgis();
    			
    			// surfaces need a special treatment
    			// Das vorgehende 'writeToPostgis()' schreibt
    			// eigentlich die letzte Tabelle. FAlls die Surface-
    			// Tabelle keine Geometrien hat, wird sie solange nicht
    			// geschrieben bis wieder eine Surface-Tabelle kommt.
    			// Somit ist es möglich, dass sie gar nicht mehr 
    			// geschrieben wird (wenn keine weiter Surface-
    			// Tabelle kommt.
    			logger.info(surfaceMainFeatureName);
    			try {
    				if (surfaceMainFeatures.size() > 0) {
    					SimpleFeatureCollection surfaceMainCollection = DataUtilities.collection( surfaceMainFeatures );
    					
    					writeToPostgis(surfaceMainCollection, surfaceMainFeatureName);
    					surfaceMainFeatures.clear();
    				}
    			} catch (Exception e) {
    				// do nothing
    			}

    			datastore.dispose();
   
    			break;
    		}
    		
    		try {
    			event = ioxReader.read();
    		} catch (IoxException ex) {
    			logger.error("Fehler beim Lesen.");
    			logger.error(ex.getMessage());
    			ex.printStackTrace();
    		}
    	}                                               

    	logger.debug("Committing Transaction...");
    	
    	try {
        	try {
        		t.commit();
        	} catch (IOException ex) {
        		logger.error("Cannot commit transaction.");
        		logger.error(ex.getMessage());
        		ex.printStackTrace();
        		
        		t.rollback();
        	} finally {
        		t.close();
        	}
    	} catch (IOException ex) {
    		logger.error(ex.getMessage());
    	}
    }
    
    private void readObject(IomObject iomObj, String featureName) throws IOException {
    	String tag=iomObj.getobjecttag();

    	if (!featureName.equalsIgnoreCase(this.featureName)) {    		
    		logger.debug("neu: " + featureName);
    		logger.debug("alt: " + this.featureName);
    		
    		if (features != null) {	
    			SimpleFeatureCollection fc = DataUtilities.collection(features);
    			//logger.debug("ich schreibe... fc: " + fc.getSchema().getTypeName() + " | Grösse: " + fc.size());
    			writeToPostgis();
    		}
    		features = new ArrayList();
    	}
    	
    	//logger.debug("Feature: " + tag);
    	SimpleFeatureType ft = (SimpleFeatureType) featureTypes.get(tag);
    	if (ft != null) {	
    		SimpleFeatureBuilder featBuilder = new SimpleFeatureBuilder( ft );	

    		String tid = null;
    		
    		if(((ItfReader) ioxReader).isRenumberTids()) {
    			tid = getTidOrRef(iomObj.getobjectoid());
    		} 
    		else {
    			tid = iomObj.getobjectoid();
    		}
    		featBuilder.set("tid", tid);

    		Object tableObj = tag2class.get(iomObj.getobjecttag());          
    		
    		if (tableObj instanceof AbstractClassDef) {
    			AbstractClassDef tableDef = (AbstractClassDef) tag2class.get(iomObj.getobjecttag());
    			ArrayList attrs = ch.interlis.iom_j.itf.ModelUtilities.getIli1AttrList(tableDef);

    			Iterator attri = attrs.iterator(); 
    			
    			while (attri.hasNext()) { 
    				ViewableTransferElement obj = (ViewableTransferElement)attri.next();

    				if (obj.obj instanceof AttributeDef) {
    					AttributeDef attr = (AttributeDef) obj.obj;
    					Type type = attr.getDomainResolvingAliases();                          
    					String attrName = attr.getName();
    					//logger.debug("attrName (AbstractClassDef): " + attrName);

    					// what is this good for?
    					if (type instanceof CompositionType) {
            				logger.debug("CompositionType");
            				int valuec = iomObj.getattrvaluecount(attrName);
            				
            				for (int valuei = 0; valuei < valuec; valuei++) {
            					IomObject value = iomObj.getattrobj(attrName, valuei);
            					if (value != null ) {
            						// do nothing...                                                        
            					}
            				}
            			}
    					else if (type instanceof PolylineType) {                               
            				IomObject value = iomObj.getattrobj(attrName, 0);
            				
            				if (value != null) {
            					featBuilder.set(attrName.toLowerCase(), Iox2wkt.polyline2jts(value, 0));
            				} else {
            					featBuilder.set(attrName.toLowerCase(), null);
            				}
            			}
    					else if (type instanceof SurfaceType) {        				
           					isSurfaceMain = true;
    						geomName = attrName.toLowerCase();
           				}
    					else if (type instanceof AreaType) {
    						isAreaMain = true;
        					IomObject value = iomObj.getattrobj(attrName, 0);
        					
        					try {
        						Point point = new GeometryFactory().createPoint(Iox2jts.coord2JTS(value));
        						//point.setSRID( Integer.parseInt(this.epsg) ); 
        						featBuilder.set(attrName.toLowerCase() + "_point", point);
        						geomName = attrName.toLowerCase();
        					} catch (Iox2jtsException e) {
        						e.printStackTrace();
        						logger.warn(e.getMessage());
        					}
    					}
    					else if (type instanceof CoordType) {
            				IomObject value = iomObj.getattrobj(attrName, 0);
            				
            				if(value != null) {
            					if(!value.getobjecttag().equals("COORD")) {
            						logger.warn("object tag <> COORD");
            					} else {
            						try {
            							Point point = new GeometryFactory().createPoint(Iox2jts.coord2JTS(value ));
            							featBuilder.set( attrName.toLowerCase(), point );
            						} catch (Iox2jtsException e) {
            							e.printStackTrace();
                						logger.warn( e.getMessage() );
            						}                                                       
            					}
            				}
            			}
    					else if (type instanceof NumericType) {
            				String value = iomObj.getattrvalue(attrName);            				
            				if (value != null) {
            					featBuilder.set(attrName.toLowerCase(), Double.valueOf(value));
            				} else {
            					featBuilder.set(attrName.toLowerCase(), null);
            				}
            			}
    					else if (type instanceof EnumerationType) {
            				String value = iomObj.getattrvalue(attrName);
            				if (value != null) {
            					featBuilder.set(attrName.toLowerCase(), Integer.valueOf( value ));
            					if (true == true) {
            						featBuilder.set( attrName.toLowerCase()+"_txt", enumCodeMapper.mapItfCode2XtfCode((EnumerationType) type, value));
            					}
            				} else {
            					featBuilder.set( attrName.toLowerCase(), null);
            					if (true == true) {
            						featBuilder.set( attrName.toLowerCase()+"_txt", null );
            					}
            				}
            			} 
    					else {
    						String value = iomObj.getattrvalue(attrName);
    						if (value != null) 
    						{
    							featBuilder.set(attrName.toLowerCase(), value);
    						} else {
    							featBuilder.set(attrName.toLowerCase(), null);
    						}
    					} 
    				}
    				if (obj.obj instanceof RoleDef) {
    	    			RoleDef role = (RoleDef) obj.obj;
    	    			String roleName = role.getName();
    	    			//logger.debug("roleName: " + roleName);

    	    			IomObject structvalue = iomObj.getattrobj(roleName, 0);
    	    			String refoid = "";
    	    			if (structvalue != null) {
    	    				if (((ItfReader) ioxReader).isRenumberTids()) 
    	    				{
    	    					refoid = getTidOrRef(structvalue.getobjectrefoid());
    	    				} else 
    	    				{
    	    					refoid = structvalue.getobjectrefoid();
    	    				}
    	    			}
    	    			featBuilder.set(roleName.toLowerCase(), refoid.toString());
    	    		}
    			}
    		}
			else if (tableObj instanceof LocalAttribute) 
			{                  
				LocalAttribute localAttr = (LocalAttribute) tag2class.get(iomObj.getobjecttag());        
				Type type = localAttr.getDomainResolvingAliases();                      
				//logger.debug("LocalAttr: " + localAttr.toString());
				
				if (type instanceof SurfaceType) {
					isSurfaceHelper = true;
					geomName = localAttr.getName().toLowerCase();
					
					String fkName = ch.interlis.iom_j.itf.ModelUtilities.getHelperTableMainTableRef(localAttr);
					IomObject structvalue=iomObj.getattrobj(fkName, 0);
					String refoid = null;
					
					if (((ItfReader) ioxReader).isRenumberTids()) {
						refoid = getTidOrRef(structvalue.getobjectrefoid());
					} 
					else {
						refoid = structvalue.getobjectrefoid();
					}		
					featBuilder.set("_itf_ref", refoid);
					
    				IomObject value = iomObj.getattrobj(ch.interlis.iom_j.itf.ModelUtilities.getHelperTableGeomAttrName( localAttr ), 0);
    				
    				if (value != null) {
    					PrecisionDecimal maxOverlaps = ((SurfaceType) type).getMaxOverlap();
    					if (maxOverlaps == null) {
        					featBuilder.set(localAttr.getName().toLowerCase(), Iox2wkt.polyline2jts( value, 0.02 ));
    					} else {
        					featBuilder.set(localAttr.getName().toLowerCase(), Iox2wkt.polyline2jts( value, maxOverlaps.doubleValue() ) );
    						//featBuilder.set(localAttr.getName().toLowerCase(), Iox2wkt.polyline2jts( value, 0.001 ));
    					}
    				}
				} 
				else if (type instanceof AreaType) {                                                                  
					isAreaHelper = true;
					geomName = localAttr.getName().toLowerCase();

					String fkName = ch.interlis.iom_j.itf.ModelUtilities.getHelperTableMainTableRef(localAttr);
					IomObject structvalue=iomObj.getattrobj(fkName, 0);
					
					featBuilder.set("_itf_ref", null);                                  
					
					IomObject value = iomObj.getattrobj(ch.interlis.iom_j.itf.ModelUtilities.getHelperTableGeomAttrName(localAttr ), 0);
					if (value != null) {
						PrecisionDecimal maxOverlaps = ((AreaType) type).getMaxOverlap();
						if (maxOverlaps == null) 
						{
							featBuilder.set(localAttr.getName().toLowerCase(), Iox2wkt.polyline2jts(value, 0.02));
						} else {
							featBuilder.set(localAttr.getName().toLowerCase(), Iox2wkt.polyline2jts(value, maxOverlaps.doubleValue()));
    						//featBuilder.set(localAttr.getName().toLowerCase(), Iox2wkt.polyline2jts( value, 0.001 ) );
						}
					}                                                               
				}				
			}
    		
        	SimpleFeature feature = featBuilder.buildFeature( null );
        	features.add(feature);
    	}  	
    }
       
    private void writeToPostgis() throws IOException {
    	//logger.debug("writeToPostgis (Geknorze.....)");
		if (isAreaHelper == true) {
			areaHelperFeatures.addAll( features );
			
			isAreaHelper = false;
		}
		else if (isAreaMain == true) {
			if (areaHelperFeatures == null) {
				SimpleFeatureCollection collection = DataUtilities.collection(features);
				writeToPostgis(collection, featureName);
			} 
			else {
				if (areaHelperFeatures.size() == 0) {
					SimpleFeatureCollection collection = DataUtilities.collection(features);
					writeToPostgis(collection, featureName);
				} else {
					SimpleFeatureCollection areaHelperCollection = DataUtilities.collection(areaHelperFeatures);
					SimpleFeatureCollection collection = DataUtilities.collection(features);
					//logger.debug("build area");
    				SimpleFeatureCollection areaCollection = SurfaceAreaBuilder.buildArea(collection, areaHelperCollection);
					//logger.debug("done");

    				writeToPostgis(areaCollection, featureName);
    				areaHelperFeatures.clear();
				}
			}
			isAreaHelper = false;
			isAreaMain = false;
		}
		else if (isSurfaceMain == true) {
			
			//logger.debug("isSurfaceMain = true");
			
			// Problem bei zwei aufeinanderfolgenden Surface-Tabellen.
			// Falls die erste KEINE Geometrie hat (keine Helper-Tabelle),
			// wird sie nicht in die DB geschrieben.
			// Wurde sie geschrieben, sollte die Anzahl = 0 sein.
			// Falls ungleich 0 -> in die DB schreiben.
			
			try {
				SimpleFeatureCollection surfaceMainCollection = DataUtilities.collection( surfaceMainFeatures );
				
				writeToPostgis(surfaceMainCollection, surfaceMainFeatureName);
				surfaceMainFeatures.clear();
			} catch (NullPointerException e) {
				System.err.println("surfaceMainCollection keine features");
				logger.warn( "no features in surfaceMainCollection" );
			}
			surfaceMainFeatures.addAll( features );
			
			surfaceMainFeatureName = featureName;
			isSurfaceMain = false;
		}
		else if (isSurfaceHelper == true) {
			logger.debug("isSurfaceHelper = true");

			SimpleFeatureCollection surfaceMainCollection = DataUtilities.collection(surfaceMainFeatures);
			SimpleFeatureCollection collection = DataUtilities.collection(features);
			//logger.debug("build surface");
			SimpleFeatureCollection coll = SurfaceAreaBuilder.buildSurface(surfaceMainCollection, collection);
			//logger.debug("done");
			
			writeToPostgis(coll, surfaceMainFeatureName);
			surfaceMainFeatures.clear();
			
			isSurfaceHelper = false;
			isSurfaceMain = false;
		}
		else {
			logger.debug("writeToPostgis: " + featureName);
			SimpleFeatureCollection collection = DataUtilities.collection(features);
			writeToPostgis(collection, featureName);			
		}
		features.clear();
	}
    

    private void writeToPostgis( SimpleFeatureCollection collection, String featureName ) throws IOException {
    	try {
    		Map params= new HashMap();
    		params.put("dbtype", "postgis");        
    		params.put("host", this.dbhost);        
    		params.put("port", this.dbport);  
    		params.put("database", this.dbname); 
    		params.put("schema", this.dbschema);
    		params.put("user", this.dbadmin);        
    		params.put("passwd", this.dbadminpwd); 
    		params.put(PostgisNGDataStoreFactory.VALIDATECONN, true);
    		params.put(PostgisNGDataStoreFactory.MAX_OPEN_PREPARED_STATEMENTS, 50);
    		params.put(PostgisNGDataStoreFactory.LOOSEBBOX, true);
    		params.put(PostgisNGDataStoreFactory.PREPARED_STATEMENTS, true);

    		if (datastore == null) {
        		datastore = new PostgisNGDataStoreFactory().createDataStore(params);
    		}

//    		if (featureName.equalsIgnoreCase("DM01AVCH24D.Gemeindegrenzen.GEMNachfuehrung")) {
//    			logger.info("fubar");
//    		}
    		
    		String tableName = (featureName.substring(featureName.indexOf(".") + 1 )).replace(".", "_").toLowerCase();

    		FeatureSource<SimpleFeatureType, SimpleFeature> source = datastore.getFeatureSource(tableName);
    		FeatureStore<SimpleFeatureType, SimpleFeature> store = (FeatureStore<SimpleFeatureType, SimpleFeature>) source;

    		store.setTransaction(t);

    		try {    				
    		    logger.info("Add features: " + featureName);
    		    store.addFeatures(collection);

    		} catch (IOException ex) {
    		    ex.printStackTrace();
    		    logger.error( ex.getMessage() );
    		}
    	} 
    	catch (IOException ex) 
    	{
    		ex.printStackTrace();
			logger.error(ex.getMessage());
            throw new IOException();
    	} 
    }
    
    
    private void deletePostgresSchemaAndTables() throws ClassNotFoundException, SQLException {
    	logger.info("Start deleting schema and tables...");
    	
    	String sql = "DROP SCHEMA IF EXISTS " + dbschema + " CASCADE;";
    	
    	Class.forName("org.postgresql.Driver");	
		Connection conn = DriverManager.getConnection("jdbc:postgresql://"+dbhost+"/"+dbname, dbadmin, dbadminpwd);    	

		Statement s = null;
		s = conn.createStatement();
		int m = 0;
		m = s.executeUpdate(sql);
    	
		logger.info("Schema and tables deleted.");
    }
    
    
    private void createPostgresSchemaAndTables() throws IOException, ClassNotFoundException, SQLException {
		logger.info("Start creating schema and tables...");
    	
    	StringBuffer buf = PGUtils.getSqlCreateStatements(iliTd, dbschema, dbadmin, dbuser, epsg, enumerationText);
    	
    	Class.forName("org.postgresql.Driver");	
		Connection conn = DriverManager.getConnection("jdbc:postgresql://"+dbhost+"/"+dbname, dbadmin, dbadminpwd);    	

		Statement s = null;
		s = conn.createStatement();
		int m = 0;
		m = s.executeUpdate(buf.toString());
    	
		logger.info("Schema and tables created.");
    }
    
    
    private String getTidOrRef(String oriTid) {
    	if (this.prefix != null) {
    		
    		if (this.prefix.length() > 0) {
    			int tidc = oriTid.length();
    			
    			for (int i = 0 ; i < (8-tidc); i++) {
    				oriTid = "0" + oriTid ;
    			}       
    		}
    	}
    	
    	if (this.prefix == null) {
    		prefix = ""; 
    	}
    	return this.prefix + oriTid;
    }
    
    
    public void setTidPrefix(String prefix) {
        this.prefix = prefix;
    }
    
    private void compileModel() throws Ili2cException {
    	IliManager manager = new IliManager();
    	String repositories[] = new String[]{"http://www.sogeo.ch/models/", "http://models.geo.admin.ch/"};
    	manager.setRepositories( repositories );
    	ArrayList modelNames = new ArrayList();
    	modelNames.add(this.modelName);
    	Configuration config = manager.getConfig(modelNames, 1.0);
    	iliTd = Ili2c.runCompiler(config);
    	
       	if (iliTd == null) {
    		throw new IllegalArgumentException("INTERLIS compiler failed");
    	}
    	logger.debug("interlis model compiled");

    	tag2class = ch.interlis.iom_j.itf.ModelUtilities.getTagMap(iliTd);
    }

    // move this to IliUtils.java 
    private void getModelNameFromItf() {
    	ItfReader ioxReader = null;
    	
    	try {
    		ioxReader = new ch.interlis.iom_j.itf.ItfReader(new java.io.File(importItfFile));
    		IoxEvent event = ioxReader.read();
    		StartBasketEvent be = null;
    		
    		do {
    			event = ioxReader.read();
    			if (event instanceof StartBasketEvent) {
    				be = (StartBasketEvent) event;
    				break;
    			}
    		}

    		while (!( event instanceof EndTransferEvent));
    		ioxReader.close();
    		ioxReader = null;

    		if (be == null) {
    			logger.error("no baskets in transfer-file"); 
    			throw new IllegalArgumentException("no baskets in transfer-file"); 
    		} 
    		else {
    			String namev[] = be.getType().split("\\."); 
    			this.modelName = namev[0]; 
    		}
    	}
    	catch (IoxException ex) { 
    		logger.error(ex.getMessage()); 
    	} 
    	finally {  

    		if (ioxReader != null)
    		{ 
    			try
    			{ 
    				ioxReader.close(); 
    			}
    			catch (IoxException ex)
    			{ 
    				logger.error(ex.getMessage()); 
    			} 
    			ioxReader = null; 
    		}
    	} 

    }
    
    private void readParams() {		
    	dbhost = (String) params.get("dbhost");
		if (dbhost == null) {
			throw new IllegalArgumentException("'dbhost' not set.");
		}	
		
    	dbport = (String) params.get("dbport");
		if (dbport == null) {
			throw new IllegalArgumentException("'dbport' not set.");
		}		
		
    	dbname = (String) params.get("dbname");
		if (dbname == null) {
			throw new IllegalArgumentException("'dbname' not set.");
		}	
		
    	dbschema = (String) params.get("dbschema");
		if (dbschema == null) {
			throw new IllegalArgumentException("'dbschema' not set.");
		}			

    	dbuser = (String) params.get("dbuser");
		if (dbuser == null) {
			throw new IllegalArgumentException("'dbuser' not set.");
		}	
    	
    	dbpwd = (String) params.get("dbpwd");
		if (dbpwd == null) {
			throw new IllegalArgumentException("'dbpwd' not set.");
		}			
		
    	dbadmin = (String) params.get("dbadmin");
		if (dbadmin == null) {
			throw new IllegalArgumentException("'dbadmin' not set.");
		}	
    	
    	dbadminpwd = (String) params.get("dbadminpwd");
		if (dbadminpwd == null) {
			throw new IllegalArgumentException("'dbadminpwd' not set.");
		}	 
		
		importModelName = (String) params.get("importModelName");
		if (importModelName == null) {
			throw new IllegalArgumentException("'importModelName' not set.");
		}
		
		importItfFile = (String) params.get("importItfFile");
		if (importItfFile == null) {
			throw new IllegalArgumentException("'importItfFile' not set.");
		}
		
		enumerationText = (Boolean) params.get("enumerationText");

		renumberTid = (Boolean) params.get("renumberTid");
		
		epsg = (String) params.get("epsg");
		if (epsg == null) {
			epsg = "21781";
		}
	}    
    
}
