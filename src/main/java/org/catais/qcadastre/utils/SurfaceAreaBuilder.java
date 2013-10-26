package org.catais.qcadastre.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureCollections;
import org.geotools.feature.FeatureIterator;
import org.geotools.data.DataUtilities;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.prep.PreparedPoint;
import com.vividsolutions.jts.geom.util.LinearComponentExtracter;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.operation.polygonize.Polygonizer;

public class SurfaceAreaBuilder {

	private static Logger logger = Logger.getLogger(SurfaceAreaBuilder.class);

	private SurfaceAreaBuilder(){};
	
	public static SimpleFeatureCollection buildSurface( SimpleFeatureCollection surfaceMainCollection, SimpleFeatureCollection surfaceHelperCollection ) 
	{
		logger.setLevel(Level.INFO);

		SimpleFeatureCollection collection = null;
		ArrayList featureList = new ArrayList();

		HashMap features = new HashMap();

		if( surfaceMainCollection != null && surfaceHelperCollection != null ) 
		{
			String geomName = surfaceHelperCollection.getSchema().getGeometryDescriptor().getName().toString();

			// Main-Table Features in HashMap mit
			// Key = TID.
			FeatureIterator i = surfaceMainCollection.features();		
			try 
			{
				while( i.hasNext() )
				{
					SimpleFeature feat = (SimpleFeature) i.next();
					String tid = (String) feat.getAttribute("tid");
					features.put( tid, feat );
			     }
			}
			finally 
			{
			     i.close();
			}
			
			// Surface polygonieren
			LinkedHashMap lines = new LinkedHashMap();
			FeatureIterator j = surfaceHelperCollection.features();		
			try 
			{
				while( j.hasNext() )
				{
					SimpleFeature feat = (SimpleFeature) j.next();
					String fk = (String) feat.getAttribute("_itf_ref");
					Geometry geom = (LineString) feat.getAttribute( geomName );
					
					if( lines.containsKey( fk ) ) 
					{
						Geometry line1 = (Geometry) lines.get(fk);
						Geometry line2 = (Geometry) line1.union(geom);
						lines.put( fk, line2 );                                          

					} else {
						lines.put( fk, geom );
					}
			     }
			}
			finally 
			{
			     j.close();
			}
			
			Iterator kt = lines.entrySet().iterator();
			while(kt.hasNext()) {
				Map.Entry kentry = (Map.Entry)kt.next();                
				String ref = kentry.getKey().toString();

				Polygonizer polygonizer = new Polygonizer();
				Geometry geom = (Geometry) kentry.getValue();

				polygonizer.add(geom);
				ArrayList polys = (ArrayList) polygonizer.getPolygons();

				// Darf eigentlich immer nur max. ein (1) Polygon
				// vorhanden sein!
				// Was aber anscheinend passieren kann, ist ein
				// LineString, der nicht polyoniert werden kann.
				// Falls keine Geometrie vorhanden ist, muss wieder
				// das original Feature geschreiben werden.

				// Wahrscheinlich macht polys.size() Probleme,
				// falls nicht grösser 0. Wird sich zeigen, bei
				// NFPerimetern!!!
				
				double exteriorPolyArea = 0;

				if(polys.size() > 0) {
					for ( int l = 0; l < polys.size(); l++ ) {
						SimpleFeature feat = (SimpleFeature) features.get(ref);
						if(feat != null) {
							Polygon poly = (Polygon) polys.get(l);
							LinearRing[] interiorRings = new LinearRing[0];
							Polygon exteriorPoly = new Polygon( (LinearRing)poly.getExteriorRing(), interiorRings, new GeometryFactory() );

							double exteriorPolyAreaTmp = exteriorPoly.getArea();
							if (exteriorPolyAreaTmp > exteriorPolyArea) 
							{
								exteriorPolyArea = exteriorPolyAreaTmp;

								feat.setAttribute(geomName, (Polygon) polys.get(l));
								features.put(ref, feat);
							}
						}
					}   
				}
			}    
			
			Iterator lt = features.entrySet().iterator();
			while(lt.hasNext()) {
				Map.Entry lentry = (Map.Entry)lt.next();  
				SimpleFeature f = (SimpleFeature) lentry.getValue();
				featureList.add(f);
			}
		}
		logger.debug( "Anzahl polygonierte/gejointe Features (Surface): " + features.size() );
		collection = DataUtilities.collection( featureList );
		return collection;
	}

	
	public static SimpleFeatureCollection buildArea( SimpleFeatureCollection areaMainCollection, SimpleFeatureCollection areaHelperCollection )
	{
		logger.setLevel(Level.INFO);
		
		SimpleFeatureCollection collection = null;
		ArrayList features = new ArrayList();
		
		if(areaMainCollection != null && areaHelperCollection != null) 
		{
			String geomName = areaHelperCollection.getSchema().getGeometryDescriptor().getName().toString();
			
			//collection = FeatureCollections.newCollection(areaMainCollection.getID());

			
			Polygonizer polygonizer = new Polygonizer();
			int inputEdgeCount = areaHelperCollection.size();
			Collection lines = getLines( areaHelperCollection );
			//logger.debug("lines: " + lines.size());
			
			Collection nodedLines = lines;
			nodedLines = nodeLines( (List) lines );

			for( Iterator it = nodedLines.iterator(); it.hasNext(); ) 
			{
				Geometry g = (Geometry) it.next();
				polygonizer.add(g);
			}

			// There can be very small holes!!!!

			ArrayList polys = (ArrayList) polygonizer.getPolygons();

			final SpatialIndex spatialIndex = new STRtree();
			for ( int i = 0; i < polys.size(); i++ ) 
			{
				Polygon p = (Polygon) polys.get(i);
				spatialIndex.insert(p.getEnvelopeInternal(), p);
			}
			logger.debug("spatial index built.");
			
			
			FeatureIterator kt = areaMainCollection.features();		
			try 
			{
				while( kt.hasNext() )
				{
					SimpleFeature feat = (SimpleFeature) kt.next();
					Geometry point = (Geometry) feat.getAttribute( geomName + "_point" );
					try 
					{
						PreparedPoint ppoint = new PreparedPoint( point.getInteriorPoint() );
						for ( final Object o : spatialIndex.query( point.getEnvelopeInternal() ) ) 
						{
							Polygon p = (Polygon) o;
							if( ppoint.intersects( p ) ) 
							{                                      
								feat.setAttribute( geomName, p );
								features.add( feat );
							}
						} 
					} catch ( NullPointerException ex ) 
					{
						ex.printStackTrace();
						logger.error( ex.getMessage() );
						logger.error( "empty point" );
					}
				}
			}
			finally 
			{
			     kt.close();
			}
		}
		logger.debug( "Anzahl polygonierte/gejointe Features (Area): " + features.size() );
		collection = DataUtilities.collection( features );
		return collection;
	}
	
	
	private static Geometry extractPoint(Collection lines)
	{
		logger.setLevel(Level.INFO);

		int minPts = Integer.MAX_VALUE;
		Geometry point = null;
		// extract first point from first non-empty geometry
		for ( Iterator i = lines.iterator(); i.hasNext(); ) 
		{
			Geometry g = (Geometry) i.next();
			if (  !g.isEmpty() ) 
			{
				Coordinate p = g.getCoordinate();
				point = g.getFactory().createPoint(p);
			}
		}
		return point;
	}
	
	
	private static Collection nodeLines( Collection lines )
	{
		logger.setLevel(Level.INFO);

	    GeometryFactory fact = new GeometryFactory();
		Geometry linesGeom = fact.createMultiLineString( fact.toLineStringArray( lines ) );
		Geometry unionInput  = fact.createMultiLineString(null);
		// force the unionInput to be non-empty if possible, to ensure union is not optimized away
		Geometry point = extractPoint(lines);
		if ( point != null )
		{
			unionInput = point;
		}
		Geometry noded = linesGeom.union( unionInput );
		List nodedList = new ArrayList();
		nodedList.add(noded);
		return nodedList;
	}
	
	
	private static Collection getLines( FeatureCollection inputFeatures )
	{
		logger.setLevel(Level.INFO);
		
		List linesList = new ArrayList();
		LinearComponentExtracter lineFilter = new LinearComponentExtracter( linesList );
		FeatureIterator i = inputFeatures.features();		
		try 
		{
			while( i.hasNext() )
			{
				SimpleFeature f = (SimpleFeature) i.next();
				Geometry g = (Geometry) f.getDefaultGeometry();
				g.apply( lineFilter );
		     }
		}
		finally 
		{
		     i.close();
		}
		return linesList;
	}
	
	
}
