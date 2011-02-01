/*
 *   Copyright panFMP Developers Team c/o Uwe Schindler
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import java.util.*;
import javax.servlet.*;
import javax.servlet.http.*;
import javax.xml.transform.*;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;
import org.xml.sax.*;
import org.xml.sax.helpers.*;

import de.pangaea.metadataportal.search.*;
import org.apache.lucene.search.*;

public class SimpleWFSServlet extends HttpServlet {

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);

		configFile=config.getInitParameter("configFile");
		if (configFile!=null) {
			String webinf=config.getServletContext().getRealPath("/WEB-INF/");
			if (webinf==null)
				throw new ServletException("Cannot resolve WEB-INF directory!");
			try {
				configFile=(new java.io.File(webinf,configFile)).getCanonicalPath();
			} catch (java.io.IOException ioe) {
				throw new ServletException("Cannot resolve config file relative to WEB-INF directory!");
			}
		} else throw new ServletException("Missing servlet parameter 'configFile'.");

		westBoundLongitudeField=config.getInitParameter("westBoundLongitudeField");
		eastBoundLongitudeField=config.getInitParameter("eastBoundLongitudeField");
		southBoundLatitudeField=config.getInitParameter("southBoundLatitudeField");
		northBoundLatitudeField=config.getInitParameter("northBoundLatitudeField");
		if (westBoundLongitudeField==null || eastBoundLongitudeField==null || southBoundLatitudeField==null || northBoundLatitudeField==null)
			throw new ServletException("Missing field names for bounding box.");
	}

	private static final void print(ContentHandler out, String text) throws SAXException {
		out.characters(text.toCharArray(),0,text.length());
	}

	private static EnumSet<FeatureType> extractFeatures(String typeNames) throws WFSException {
		// return all features if parameter is null
		if (typeNames==null) return EnumSet.allOf(FeatureType.class);
		// generate set of features starting with an empty one
		EnumSet<FeatureType> features=EnumSet.noneOf(FeatureType.class);
		StringTokenizer st=new StringTokenizer(typeNames,",");
		while (st.hasMoreTokens()) {
			String fname=st.nextToken();
			try {
				features.add(FeatureType.valueOf(fname));
			} catch (IllegalArgumentException e) {
				throw new WFSException("Invalid feature type '"+fname+"' supplied in TYPENAME parameter. Allowed ones are "+Arrays.toString(FeatureType.values()));
			}
		}
		return features;
	}

	private void returnError(ContentHandler out, String error) throws SAXException {
		AttributesImpl atts=new AttributesImpl();
		atts.addAttribute("","","xmlns",CDATA,"http://www.opengis.net/ogc");
		atts.addAttribute("","","xmlns:xsi",CDATA,"http://www.w3.org/2001/XMLSchema-instance");
		atts.addAttribute("","","xsi:schemaLocation",CDATA,"http://www.opengis.net/ogc http://schemas.opengis.net/wfs/1.0.0/OGC-exception.xsd");
		atts.addAttribute("","","version",CDATA,"1.2.0");
		out.startElement("","","ServiceExceptionReport",atts);
		atts.clear();
		out.startElement("","","ServiceException",atts);
		print(out,error);
		out.endElement("","","ServiceException");
		out.endElement("","","ServiceExceptionReport");
	}

	private void doGetFeature(ContentHandler out, Map<String,String> params, SearchService service, Query query) throws Exception {
		// check params
		EnumSet<FeatureType> featureTypes=extractFeatures(params.get("TYPENAME"));

		// get limit
		int limit=Integer.MAX_VALUE;
		String s=params.get("MAXFEATURES");
		if (s!=null && !"".equals(s)) try {
			limit=Integer.parseInt(s);
			if (limit<=0) throw new WFSException("MAXFEATURES is not a positive value: "+s);
		} catch (NumberFormatException ne) {
			throw new WFSException("MAXFEATURES is not a number: "+s);
		}

		// get BBOX
		s=params.get("BBOX");
		if (s!=null) try {
			Double[] bbox=new Double[4]; // west,south,east,north
			Arrays.fill(bbox,null);
			StringTokenizer st=new StringTokenizer(s,",");
			for(int i=0; i<4; i++) {
				if (!st.hasMoreTokens()) throw new WFSException("BBOX incomplete: "+s);
				String s1=st.nextToken().trim();
				if (!"".equals(s1)) bbox[i]=new Double(s1); else throw new WFSException("BBOX invalid: "+s);
			}
			if (st.hasMoreTokens()) throw new WFSException("BBOX has too many values: "+s);
			Double LongitudeW=bbox[0],LatitudeS=bbox[1],LongitudeE=bbox[2],LatitudeN=bbox[3];

			// create new BooleanQuery containing given user-query and BBOX
			BooleanQuery bq=service.newBooleanQuery();
			// append original query
			bq.add(query, BooleanClause.Occur.MUST);
			// append bounding box (we create parts of the query in a way that all datasets *overlapping*
			// the bounding box are fetched, this is why all ranges are half-open;
			// draw a figure to understand how the query works ;-)
			bq.add(service.newNumericRangeQuery(southBoundLatitudeField, null, LatitudeN), BooleanClause.Occur.MUST);
			bq.add(service.newNumericRangeQuery(northBoundLatitudeField, LatitudeS, null), BooleanClause.Occur.MUST);
			bq.add(service.newNumericRangeQuery(westBoundLongitudeField, null, LongitudeE), BooleanClause.Occur.MUST);
			bq.add(service.newNumericRangeQuery(eastBoundLongitudeField, LongitudeW, null), BooleanClause.Occur.MUST);
			// replace original query by extended one
			query=bq;
		} catch (NumberFormatException ne) {
			throw new WFSException("BBOX contains an invalid number format: "+s);
		}

		// start output
		AttributesImpl atts=new AttributesImpl();
		atts.addAttribute("","","xmlns:gml",CDATA,"http://www.opengis.net/gml");
		atts.addAttribute("","","xmlns:ds",CDATA,"urn:java:"+getClass().getName());
		atts.addAttribute("","","xmlns:wfs",CDATA,"http://www.opengis.net/wfs");
		out.startElement("","","wfs:FeatureCollection",atts);

		// fill the mandatory bounding box around all datasets, haha
		out.startElement("","","gml:boundedBy",EMPTY_ATTS);
		out.startElement("","","gml:Box",SRS_ATTS);
		out.startElement("","","gml:coordinates",EMPTY_ATTS);
		print(out,"-180.0,-90.0 180.0,90.0");
		out.endElement("","","gml:coordinates");
		out.endElement("","","gml:Box");
		out.endElement("","","gml:boundedBy");

		Collector coll=new Collector(out,limit,featureTypes);
		service.search(coll,query,false,southBoundLatitudeField,northBoundLatitudeField,westBoundLongitudeField,eastBoundLongitudeField);
		if (coll.exception!=null) throw coll.exception; // throw exception occurred inside collector

		out.endElement("","","wfs:FeatureCollection");
	}

	private ContentHandler getContentHandler(HttpServletResponse resp) throws ServletException,java.io.IOException,SAXException,TransformerConfigurationException {
		resp.setContentType("text/xml; charset="+CHARSET);
		SAXTransformerFactory transFactory=(SAXTransformerFactory)SAXTransformerFactory.newInstance();
		TransformerHandler out=transFactory.newTransformerHandler();
		out.setResult(new StreamResult(resp.getOutputStream()));
		Transformer trans=out.getTransformer();
		trans.setOutputProperty(OutputKeys.ENCODING,CHARSET);
		trans.setOutputProperty(OutputKeys.INDENT,"no");
		trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,"no");
		out.startDocument();
		return out;
	}

	private void doRequest(HttpServletRequest req, HttpServletResponse resp, Map<String,String> params, ContentHandler out) throws ServletException,java.io.IOException,Exception {
		String id=req.getPathInfo();
		SearchService searchService=null;
		Query query=null;
		try {
			// pathinfo consists of "/indexname/queryhash"
			if (id==null) throw new NoSuchElementException();
			StringTokenizer st=new StringTokenizer(id,"/");
			searchService=new SearchService(configFile, st.nextToken());
			query=searchService.readStoredQuery(UUID.fromString(st.nextToken()));
			if (query==null || st.hasMoreTokens()) throw new NoSuchElementException();
		} catch (NoSuchElementException e) {
			returnError(out,"Missing index name or query hash in request URI.");
			return;
		} catch (Exception e) {
			returnError(out,"The WebFeatureService was not able to handle your request: "+e);
			return;
		}

		try {
			String version=params.get("VERSION");
			if (version!=null && !version.equals("1.0.0")) throw new WFSException("VERSION not supported: "+version);
			String service=params.get("SERVICE");
			if (service!=null && !service.equals("WFS")) throw new WFSException("SERVICE not supported: "+service);

			String request=params.get("REQUEST");
			if (request==null) throw new WFSException("You must supply a valid REQUEST!");
			else if (request.equals("GetFeature")) doGetFeature(out,params,searchService,query);
			else throw new WFSException("REQUEST is not supported because this is a *minimal* implementation of a WFS service only supporting GETFEATURE.");
		} catch (WFSException e) {
			returnError(out,e.getMessage());
		}
	}

	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,java.io.IOException {
		req.setCharacterEncoding(PARAM_CHARSET);

		// convert parameters in a WFS compatible way (all parameter names are case-insensitive)
		HashMap<String,String> params=new HashMap<String,String>();
		@SuppressWarnings("unchecked") Enumeration<String> e=(Enumeration<String>)req.getParameterNames();
		while (e.hasMoreElements()) {
			String key=e.nextElement();
			String value=req.getParameter(key);
			// some WFS client implementations create additional '?' in query string which are wrong, strip them
			if (key.startsWith("?")) key=key.substring(1);
			if (value!=null) params.put(key.toUpperCase(Locale.ENGLISH),value);
		}

		ContentHandler out=null;
		try {
			out=getContentHandler(resp);
			doRequest(req,resp,params,out);
			out.endDocument();
		} catch (Exception ex) {
			throw new ServletException(ex);
		}
	}

	protected String configFile=null,westBoundLongitudeField=null,eastBoundLongitudeField=null,southBoundLatitudeField=null,northBoundLatitudeField=null;

	protected static final String CHARSET="UTF-8";
	protected static final String PARAM_CHARSET="UTF-8";
	protected static final String CDATA="CDATA";
	protected static final AttributesImpl EMPTY_ATTS=new AttributesImpl();
	protected static final AttributesImpl SRS_ATTS=new AttributesImpl();
	static {
		SRS_ATTS.addAttribute("","","srsName",CDATA,"EPSG:4326"); // WGS84 SRS with Lat/Lon
	}

	// inner classes
	protected static enum FeatureType { POINT, POLYGON };

	protected static class WFSException extends Exception {

		public WFSException(String msg) {
			super(msg);
		}

	}

	protected class Collector implements SearchResultCollector {

		public Collector(ContentHandler out, int limit, EnumSet<FeatureType> featureTypes) {
			this.out=out;
			this.limit=limit;
			this.featureTypes=featureTypes;
		}

		public boolean collect(SearchResultItem res) {
			String id=res.getIdentifier();

			// There may be more than one coordinate per field, if source XML contained more than one bounding box (seldom).
			// TODO: In this case we do not use the collected hit
			Object[] aLongitudeW=res.getField(westBoundLongitudeField);
			Double LongitudeW=(aLongitudeW==null || aLongitudeW.length!=1) ? null : (Double)aLongitudeW[0];
			Object[] aLongitudeE=res.getField(eastBoundLongitudeField);
			Double LongitudeE=(aLongitudeE==null || aLongitudeE.length!=1) ? null : (Double)aLongitudeE[0];
			Object[] aLatitudeS=res.getField(southBoundLatitudeField);
			Double LatitudeS=(aLatitudeS==null || aLatitudeS.length!=1) ? null : (Double)aLatitudeS[0];
			Object[] aLatitudeN=res.getField(northBoundLatitudeField);
			Double LatitudeN=(aLatitudeN==null || aLatitudeN.length!=1) ? null : (Double)aLatitudeN[0];

			// if any of the values is null, stop processing of collected hit
			if (LatitudeN==null || LatitudeS==null || LongitudeW==null || LongitudeE==null) return true; // next one

			FeatureType aktFeatureType=(LatitudeN.equals(LatitudeS) && LongitudeW.equals(LongitudeE)) ? FeatureType.POINT : FeatureType.POLYGON;
			try {
				// print Feature
				if (featureTypes.contains(aktFeatureType)) {
					out.startElement("","","gml:featureMember",EMPTY_ATTS);
					out.startElement("","","ds:dataSet",EMPTY_ATTS);
					out.startElement("","","ds:URI",EMPTY_ATTS);
					print(out,id);
					out.endElement("","","ds:URI");
					out.startElement("","","ds:location",EMPTY_ATTS);
					switch (aktFeatureType) {
						case POINT:
							out.startElement("","","gml:Point",SRS_ATTS);
							out.startElement("","","gml:coordinates",EMPTY_ATTS);
							print(out,LongitudeW+","+LatitudeS);
							out.endElement("","","gml:coordinates");
							out.endElement("","","gml:Point");
							break;
						case POLYGON:
							out.startElement("","","gml:Polygon",SRS_ATTS);
							out.startElement("","","gml:outerBoundaryIs",EMPTY_ATTS);
							out.startElement("","","gml:LinearRing",EMPTY_ATTS);
							out.startElement("","","gml:coordinates",EMPTY_ATTS);
							print(out,LongitudeW+","+LatitudeS+" ");
							print(out,LongitudeW+","+LatitudeN+" ");
							print(out,LongitudeE+","+LatitudeN+" ");
							print(out,LongitudeE+","+LatitudeS+" ");
							print(out,LongitudeW+","+LatitudeS);
							out.endElement("","","gml:coordinates");
							out.endElement("","","gml:LinearRing");
							out.endElement("","","gml:outerBoundaryIs");
							out.endElement("","","gml:Polygon");
							break;
					}
					out.endElement("","","ds:location");
					out.endElement("","","ds:dataSet");
					out.endElement("","","gml:featureMember");
					count++;
				}
			} catch (SAXException saxe) {
				this.exception=saxe;
				return false; // cancel collection
			}
			return (count<limit);
		}

		private ContentHandler out;
		private int limit,count=0;
		private EnumSet<FeatureType> featureTypes;

		protected Exception exception=null;
	}
}
