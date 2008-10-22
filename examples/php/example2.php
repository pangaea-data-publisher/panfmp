<?php
/*
 *   Copyright 2007-2008 panFMP Developers Team c/o Uwe Schindler
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
if (isset($_GET['dataCenterFull'])) {
	$dataCenterFull=$_GET['dataCenterFull'];
	if ($dataCenterFull=='') unset($dataCenterFull);
}
$offset=isset($_GET['offset'])?$_GET['offset']:0;
$count=10; //currently fixed to 10

// Prepare XSL for HTML
$xsl=DomDocument::load('./dif2html.xslt');
$descProc=new XSLTprocessor;
$descProc->registerPHPFunctions();
$descProc->importStyleSheet($xsl);

function printNavigator($query,$dataCenterFull,$totalCount,$offset,$count) {
	$pages=$totalCount/$count;
	$page=$offset/$count;

	if ($page>0) echo "<a href=\"".htmlspecialchars($_SERVER['PHP_SELF']."?q=".urlencode($query)."&dataCenterFull=".urlencode($dataCenterFull)."&offset=".(($page-1)*$count))."\">";
	echo "&lt;&lt; PREV";
	if ($page>0) echo "</a>";

	echo " | ";
	$start=$page-10;
	if ($start<0) $start=0;
	for ($i=$start; ($i<$pages && $i<$page+10); $i++) {
		if ($i!=$page) echo "<a href=\"".htmlspecialchars($_SERVER['PHP_SELF']."?q=".urlencode($query)."&dataCenterFull=".urlencode($dataCenterFull)."&offset=".($i*$count))."\">"; else echo "<b>";
		echo $i+1;
		if ($i!=$page) echo "</a>"; else echo "</b>";
		echo " | ";
	}

	if ($page<$pages-1) echo "<a href=\"".htmlspecialchars($_SERVER['PHP_SELF']."?q=".urlencode($query)."&dataCenterFull=".urlencode($dataCenterFull)."&offset=".(($page+1)*$count))."\">";
	echo "NEXT &gt;&gt;";
	if ($page<$pages-1) echo "</a>";
}

function printForm($ws,$query,$dataCenterFull) {
?>
<form method="GET" action="<?=htmlspecialchars($_SERVER['PHP_SELF'])?>">
<p>Query:&nbsp;<input name="q" type="text" size="30" maxlength="1024" value="<?=htmlspecialchars(isset($query)?$query:'')?>" /><input type="submit" /></p>
<p>Data Center:&nbsp;<select name="dataCenterFull" size="1">
	<option<?=isset($dataCenterFull)?'':' selected'?>></option>
<?php
	$terms=$ws->listTerms('dataportal','dataCenterFull',65536);
	foreach ($terms as $term) {
		echo "    <option".((isset($dataCenterFull) && $term==$dataCenterFull)?' selected':'').">".htmlspecialchars($term)."</option>\n";
	}
?>
</select></p>
</form>
<?php
}

// print header of page
?><html>
<head>
<title>Example Data Portal</title>
</head>
<body>
<h1>Example Data Portal</h1>
<?php

try {
	$ws=new SoapClient($wsUrl,array('encoding'=>'UTF-8'));
	
	printForm($ws,$query,$dataCenterFull);

	// process results
	if (isset($query) || isset($dataCenterFull)) {
		$search=new stdClass();
		$search->index='dataportal';

		// sort, NULL means no sorting by field only default sorting by relevance (highest first)
		// if field is given then results are first sorted by this field (in order specified by boolean "sortReverse", than relevance (highest first)
		$search->sortField=NULL;
		$search->sortReverse=NULL;

		// query strings
		$search->queries=array();
		if (isset($query)) {
			$search->queries[]=$q=new stdClass();
			$q->field=NULL; // NULL means default field
			$q->query=$query;
			$q->anyOf=FALSE;
		}
		if (isset($dataCenterFull)) {
			$search->queries[]=$q=new stdClass();
			$q->field='dataCenterFull';
			$q->query=$dataCenterFull;
			$q->anyOf=FALSE;
		}
		$res=$ws->search($search,$offset,$count);

		printNavigator($query,$dataCenterFull,$res->totalCount,$offset,$count);

		echo "<ol start=\"".($res->offset+1)."\">\n";

		foreach($res->results as $item) {
			// we extend the returned XML text string by an XML header, to give the correct encoding for parsing the string with an XML parser
			$doc=DOMDocument::loadXML('<?xml version="1.0" encoding="UTF-8" ?>'.$item->xml);
			$descProc->setParameter("","score",$item->score);
			echo $descProc->transformToXML($doc);
		}

		echo "</ol>\n";

		printNavigator($query,$dataCenterFull,$res->totalCount,$offset,$count);
	}
} catch (SoapFault $e) {
	echo "<p><b><font color=red>Error:</font></b> ".htmlspecialchars($e->faultstring)."</p>";
}
?>
</body>
</html>