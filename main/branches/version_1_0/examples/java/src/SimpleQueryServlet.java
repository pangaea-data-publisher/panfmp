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

import javax.servlet.*;
import javax.servlet.http.*;

import java.util.*;
import java.io.*;
import java.net.URLEncoder;
import javax.xml.transform.*;
import javax.xml.transform.stream.*;

import de.pangaea.metadataportal.search.*;
import de.pangaea.metadataportal.utils.StaticFactories;
import org.apache.lucene.search.*;

public class SimpleQueryServlet extends HttpServlet {

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);

		// get config file
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
		
		// load XSL template
		InputStream is=config.getServletContext().getResourceAsStream("/WEB-INF/dif2html.xslt"); 
		try {
			template=StaticFactories.transFactory.newTemplates(new StreamSource(is,"webinf://dif2html.xslt"));
		} catch (Exception e) {
			throw new ServletException("Failed to initialize XSL template: ",e);
		} finally {
			try { is.close(); } catch (IOException ioe) {} // ignore
		}
	}

	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,java.io.IOException {
		// init in/out
		req.setCharacterEncoding(CHARSET);
		resp.setContentType("text/html");
		resp.setCharacterEncoding(CHARSET);
		// read index name from path info
		String index=req.getPathInfo();
		if (index==null) throw new ServletException("No index given in URI.");
		index=index.substring(1); // trim "/"
		// read parameter
		String query=req.getParameter("q");
		if (query!=null && "".equals(query=query.trim())) query=null;
		int pg=1;
		String pgstr=req.getParameter("pg");
		if (pgstr!=null) pg=Integer.parseInt(pgstr);
		if (pg<=0) pg=1;
		try {
			final SearchService service=new SearchService(configFile, index);
			final PrintWriter out=resp.getWriter();
			printHeader(out);
			printForm(out,query);
			if (query!=null) {
				// build a query
				final BooleanQuery bq=service.newBooleanQuery();
				bq.add(service.newDefaultFieldQuery(query), BooleanClause.Occur.MUST);
				// e.g. add more query constraints:
				/*
				bq.add(service.newNumericRangeQuery("longitude", -20.0, 10.0), BooleanClause.Occur.MUST);
				bq.add(service.newNumericRangeQuery("latitude", null, 30.5), BooleanClause.Occur.MUST);
				*/
				// start search, return only XML and no fields
				final SearchResultList list=service.search(bq,true,Collections.<String>emptySet());
				final String wfsUrl="../wfs/"+index+"/"+service.storeQuery(bq)+"?REQUEST=GetFeature";
				printResults(out,query,wfsUrl,list,pg);
			}
			printFooter(out);
		} catch (Exception ex) {
			throw new ServletException("panFMP error: "+ex);
		}
	}
	
	// I do not understand why Java Does not support a function like this...
    private static final String xmlSpecialChars(String s) {
        final int l=s.length();
        final StringBuilder dest=new StringBuilder(l);
        for (int i=0; i<l; i++) {
            char c=s.charAt(i);
            switch (c) {
                case '<': dest.append("&lt;");break;
                case '>': dest.append("&gt;");break;
                case '"': dest.append("&quot;");break;
                case '&': dest.append("&amp;");break;
                default:  dest.append(c);
            }
        }
        return dest.toString();
    }

	private void printHeader(final PrintWriter out) {
		out.println("<html><head><title>Portal test site</title></head>");
		out.println("<body><h1>Portal test site</h1>");
	}

	private void printFooter(final PrintWriter out) {
		out.println("</body></html>");
	}

	private void printForm(final PrintWriter out, final String query) {
		out.println("<form method=\"GET\" action=\"\">");
		out.print("<p>Query:&nbsp;<input name=\"q\" type=\"text\" size=\"30\" maxlength=\"1024\" value=\"");
		if (query!=null) out.print(xmlSpecialChars(query));
		out.println("\" /><input type=\"submit\" /></p>");
		out.println("</form>");
	}
	
	private void printNavigator(final PrintWriter out, final String query, final int pg, final int pages) throws Exception {
		final String q=xmlSpecialChars("?q="+URLEncoder.encode(query,CHARSET)+"&pg=");
		
		if (pg>1) out.print("<a href=\""+q+(pg-1)+"\">");
		out.print("&lt;&lt; PREV");
		if (pg>1) out.print("</a>");
		out.print(" | ");

		int start=pg-10;
		if (start<1) start=1;
		for (int i=start; i<=pages && i<pg+10; i++) {
			if (i!=pg) out.print("<a href=\""+q+i+"\">");
			out.print(i);
			if (i!=pg) out.print("</a>");
			out.print(" | ");
		}

		if (pg<pages) out.print("<a href=\""+q+(pg+1)+"\">");
		out.print("NEXT &gt;&gt;");
		if (pg<pages) out.print("</a>");
	}

	private void printResults(final PrintWriter out, final String query, final String wfsUrl, final SearchResultList list, final int pg) throws Exception {
		// select the first page (optimally, it would be configureable from parameters)
		final int start=(pg-1)*resultsPerPage, siz=list.size(), pages=((siz+resultsPerPage-1)/resultsPerPage);
		final List<SearchResultItem> page=list.subList(Math.min(start,siz), Math.min(start+resultsPerPage,siz));
		out.print("<p>Query executed in "+list.getQueryTime()+"ms and returned "+list.size()+" results. ");
		out.print("Displaying page "+pg+" of "+pages+". ");
		out.print("<a href=\""+xmlSpecialChars(wfsUrl)+"\">Web Feature Service example</a>");
		out.println("</p>");
		if (page.size()>0) {
			printNavigator(out,query,pg,pages);
			out.println("<ul>");
			final Transformer trans=template.newTransformer();
			final StreamResult sr=new StreamResult(out);
			for (SearchResultItem item : page) {
				trans.setParameter("score", item.getScore());
				trans.transform(new StreamSource(new StringReader(item.getXml()),item.getIdentifier()), sr);
			}
			out.println("</ul>");
			printNavigator(out,query,pg,pages);
		}
	}

	private static final String CHARSET="UTF-8";

	private String configFile=null;
	private Templates template=null;
	
	public int resultsPerPage=10;
	
}
