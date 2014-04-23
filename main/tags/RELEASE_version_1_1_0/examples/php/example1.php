<?php
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

// you may change the URL to Jetty here
$wsUrl='http://127.0.0.1:8801/axis/Search?wsdl';

header('Content-Type: text/html; charset=UTF-8');
if (isset($_GET['q'])) {
	$query=$_GET['q'];
	if (trim($query)=='') unset($query);
}

?><html>
<head>
<title>Portal test site</title>
</head>
<body>
<h1>Portal test site</h1>
<h2>Query</h2>
<form method="GET" action="<?=htmlspecialchars($_SERVER['PHP_SELF'])?>">
<p>Query:&nbsp;<input name="q" type="text" size="30" maxlength="1024" value="<?=htmlspecialchars(isset($query)?$query:'')?>" /><input type="submit" /></p>
</form>
<?php

try {
	$ws=new SoapClient($wsUrl,array('encoding'=>'UTF-8','trace'=>TRUE));
	if (isset($query)) {

		// create search query
		$search=new stdClass();
		$search->index='dataportal';

		// sort, NULL means no sorting by field only default sorting by relevance (highest first)
		// if field is given then results are first sorted by this field (in order specified by boolean "sortReverse", than relevance (highest first)
		$search->sortField=NULL;
		$search->sortReverse=NULL;

		// query string(s)
		$search->queries=array();
		$search->queries[]=$q=new stdClass();
		$q->field=NULL; // NULL means default field
		$q->query=$query;
		$q->anyOf=FALSE;

		// Two examples for Range Queries (currently disabled, for demonstration puposes only)
		$search->ranges=array();
		/*
		$search->ranges[]=$r=new stdClass();
		$r->field='maxLatitude';
		$r->min=new SoapVar(-90.0, XSD_DOUBLE); // cast it explicitely to define correct type, because in WSDL it is XSD_ANYTYPE
		$r->max=NULL;
		$search->ranges[]=$r=new stdClass();
		$r->field='minLatitude';
		$r->min=NULL;
		$r->max=new SoapVar(90.0, XSD_DOUBLE); // cast it explicitely to define correct type, because in WSDL it is XSD_ANYTYPE
		*/

		// start query
		$res=$ws->search($search,0,10);

		// print result in raw form
		echo "<h2>Result</h2>\n<pre>";
		ob_start('htmlspecialchars');
		print_r($res);
		ob_end_flush();
		echo "</pre>\n";
	}

} catch (SoapFault $e) {
	echo "<p>Request:<PRE>".htmlspecialchars($ws->__getLastRequest())."</PRE>";
	echo "<p>Response:<PRE>".htmlspecialchars($ws->__getLastResponse())."</PRE>";
	echo "<p>SoapFault:<PRE>".htmlspecialchars($e->faultstring)."</PRE>";
}

?>
</body>
</html>