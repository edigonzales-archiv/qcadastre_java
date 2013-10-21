package org.catais.qcadastre.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.catais.qcadastre.App;

import ch.interlis.ili2c.Ili2cException;
import ch.interlis.ili2c.metamodel.AbstractClassDef;
import ch.interlis.ili2c.metamodel.AreaType;
import ch.interlis.ili2c.metamodel.AttributeDef;
import ch.interlis.ili2c.metamodel.CoordType;
import ch.interlis.ili2c.metamodel.Model;
import ch.interlis.ili2c.metamodel.PolylineType;
import ch.interlis.ili2c.metamodel.PredefinedModel;
import ch.interlis.ili2c.metamodel.SurfaceType;
import ch.interlis.ili2c.metamodel.Topic;
import ch.interlis.ili2c.metamodel.Type;
import ch.interlis.ili2c.metamodel.TypeModel;
import ch.interlis.ili2c.metamodel.Viewable;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


public class QGISUtils 
{
	private static Logger logger = Logger.getLogger(QGISUtils.class);

    public QGISUtils() {
    	
    }
    
    public static void createTopicsTablesJson(String importModelName, String dbschema) throws Ili2cException, IllegalArgumentException {
		logger.setLevel(Level.DEBUG);

		ch.interlis.ili2c.metamodel.TransferDescription iliTd = IliUtils.compileModel(importModelName);
		
		try {
			
            final Path tempDir = Files.createTempDirectory("qcadastre" + Math.random());
			File topicsFile = new File(tempDir.toFile(), "topics.json");

			FileWriter fw = new FileWriter(topicsFile);
			BufferedWriter bw = new BufferedWriter(fw); 
			
			JSONObject jobjIli = new JSONObject();
			jobjIli.put("model", importModelName);
			JSONArray jTopicsList = new JSONArray();
		
			Iterator modeli = iliTd.iterator();
			while (modeli.hasNext()) {
				Object mObj = modeli.next();

				if (mObj instanceof Model) {
					Model model = (Model) mObj;
					
					if (model instanceof TypeModel) {
						continue;                               
					}
					
					if (model instanceof PredefinedModel) {
						continue;
					}
					Iterator topici = model.iterator();
					
					while (topici.hasNext()) {
						Object tObj = topici.next();

						if (tObj instanceof Topic) {
							Topic topic = (Topic) tObj;
							Iterator iter = topic.getViewables().iterator();

							String topicName = topic.getName();
							logger.debug(topic.getName());
							
							JSONObject jobjTopic = new JSONObject();
							jobjTopic.put("topic", topicName);
							jobjTopic.put("title", topicName);
							jobjTopic.put("group", topicName);
							
							jTopicsList.add(jobjTopic);
							
							JSONArray jTablesList = new JSONArray();
														
							while (iter.hasNext()) {
								Object obj = iter.next();

								if (obj instanceof Viewable) {
									Viewable v = (Viewable) obj;

									if(IliUtils.isPureRefAssoc(v))
									{
										continue;
									}
									String className = v.getScopedName(null);
									logger.debug(className);
									
									String tableName = (className.substring(className.indexOf(".")+1)).replace(".", "_").toLowerCase();
																											
									ArrayList <String>geometryColumns = new ArrayList();
									Iterator attri = v.getAttributesAndRoles2();

									while (attri.hasNext()) {
										ch.interlis.ili2c.metamodel.ViewableTransferElement attrObj = (ch.interlis.ili2c.metamodel.ViewableTransferElement) attri.next();
										
										if(attrObj.obj instanceof AttributeDef) {
											AttributeDef attrdefObj = (AttributeDef) attrObj.obj;
											Type type = attrdefObj.getDomainResolvingAliases();  
											String attrName = attrdefObj.getName().toLowerCase();
											
											if (type instanceof PolylineType || type instanceof SurfaceType || type instanceof AreaType || type instanceof CoordType) {
												geometryColumns.add(attrName);												 
											}
										}
									}
									
									if (geometryColumns.size() > 0) {
										
										for (String g : geometryColumns) {
											JSONObject jobjTable = new JSONObject();
											jobjTable.put("group", topicName);
											jobjTable.put("title", v.getName() + " (" + g + ")");
											jobjTable.put("featuretype", tableName);
											jobjTable.put("key", "ogc_fid");
											jobjTable.put("geom", g);
											jobjTable.put("read-only", "true");
											jobjTable.put("style", "");
											
											jTablesList.add(jobjTable);
										}
									} else {
										JSONObject jobjTable = new JSONObject();
										jobjTable.put("group", topicName);
										jobjTable.put("title", v.getName());
										jobjTable.put("featuretype", tableName);
										jobjTable.put("key", "ogc_fid");
										jobjTable.put("geom", "");
										jobjTable.put("read-only", "true");
										jobjTable.put("style", "");
										
										jTablesList.add(jobjTable);
									}

								}
							}
							jobjTopic.put("tables", jTablesList);
						}
					}
					jobjIli.put("topics", jTopicsList);
					fw.write(jobjIli.toJSONString());
				}
			}
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
   	
    	
    }
    
    
}
