# PANGAEA® Framework for Metadata Portals

[panFMP](http://www.panfmp.org) is a generic and flexible framework for building
geoscientific metadata portals independent of content standards for metadata and protocols.
Data providers can be harvested with commonly used protocols (e.g., Open
Archives Initiative Protocol for Metadata Harvesting) and metadata standards
like Dublin Core, DIF, or ISO 19115. The new Java-based portal software
supports any XML encoding and makes metadata searchable through Apache
Lucene. Software administrators are free to define searchable fields
independent of their type using XPath and/or XSL Templates. In addition, by
extending the full-text search engine (FTS) Apache Lucene, we have
significantly improved queries for numerical and date/time ranges by
supplying a new trie-based algorithm, thus enabling high-performance
space/time retrievals in FTS-based geo portals. The harvested metadata are
stored in separate indexes, which makes it possible to combine these into
different portals. The portal-specific Java API and web service interface is
highly flexible and supports custom front-ends for users, provides automatic
query completion (AJAX), and dynamic visualization with conventional mapping
tools.

panFMP is now maintained at GitHub and uses services supplied by them
for tracking bugs, managing source code (Git). Please visit our
[documentation page](http://www.panfmp.org/front_content.php?idart=416)
for details about installing and using panFMP!

panFMP version 1.1.0 was released on 2014-04-23. You can download the
artifacts on the [GitHub releases](https://github.com/pangaea-data-publisher/panfmp/releases)
page. This version is the last version including the full search server
based on Lucene 3.6. If you want to test the latest developments, daily development
snapshots are available directly from this homepage. The new version is using
Elasticsearch to power the search engine, while panFMP will only provide harvester
and mapping of the XML documents.
