package org.catais.qcadastre.utils;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import ch.interlis.ili2c.Ili2c;
import ch.interlis.ili2c.Ili2cException;
import ch.interlis.ili2c.config.Configuration;
import ch.interlis.ili2c.metamodel.AssociationDef;
import ch.interlis.ili2c.metamodel.Viewable;
import ch.interlis.ilirepository.IliManager;

public class IliUtils 
{
	private static Logger logger = Logger.getLogger(IliUtils.class);

    public IliUtils() {
    	
    }
    
    public static ch.interlis.ili2c.metamodel.TransferDescription compileModel(String importModelName) throws Ili2cException, IllegalArgumentException  {
    	ch.interlis.ili2c.metamodel.TransferDescription iliTd = null;
    	
    	IliManager manager = new IliManager();
    	String repositories[] = new String[]{"http://www.catais.org/models/", "http://models.geo.admin.ch/"};
    	manager.setRepositories(repositories);
    	
    	ArrayList modelNames = new ArrayList();
    	modelNames.add(importModelName);
    	
    	Configuration config = manager.getConfig(modelNames, 1.0);
    	iliTd = Ili2c.runCompiler(config);

    	if (iliTd == null) {
    		throw new IllegalArgumentException("INTERLIS compiler failed");
    	}    	
    	
    	return iliTd;
    }
    
	public static boolean isPureRefAssoc(Viewable v) {
		if (!(v instanceof AssociationDef)) {
			return false;
		}
		AssociationDef assoc = (AssociationDef) v;
		// embedded and no attributes/embedded links?
		if ( assoc.isLightweight() && 
				!assoc.getAttributes().hasNext()
				&& !assoc.getLightweightAssociations().iterator().hasNext()
				) 
		{
			return true;
		}
		return false;
	}	


}
