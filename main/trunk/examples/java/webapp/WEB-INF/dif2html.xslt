<?xml version="1.0" encoding="iso-8859-1"?>
<!--
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
-->

<xsl:stylesheet version="1.0" xmlns:dif="http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:date="http://exslt.org/dates-and-times" xmlns:exsl="http://exslt.org/common" exclude-result-prefixes="exsl xs xsl date dif">
	<xsl:output encoding="UTF-8" method="xml" indent="yes" omit-xml-declaration="yes" />
	<xsl:param name="score"/>
	<xsl:param name="target" select="'_blank'"/>
	<xsl:param name="showinfo" select="true()"/>

	<xsl:variable name="url-pre">
		<xsl:for-each select="/*/dif:Related_URL[contains(dif:URL_Content_Type,'DATA')]/dif:URL[normalize-space(.)!='']">
			<token><xsl:value-of select="." /></token>
		</xsl:for-each>
		<xsl:for-each select="/*/dif:Data_Set_Citation/dif:Online_Resource[normalize-space(.)!='']">
			<token><xsl:value-of select="." /></token>
		</xsl:for-each>
		<xsl:for-each select="/*/dif:Related_URL/dif:URL[normalize-space(.)!='']">
			<token><xsl:value-of select="." /></token>
		</xsl:for-each>
	</xsl:variable>
	
	<xsl:variable name="url">
		<xsl:for-each select="exsl:node-set($url-pre)/token">
			<xsl:if test="position()=1"><xsl:value-of select="." /></xsl:if>
		</xsl:for-each>
	</xsl:variable>

	<xsl:template match="/*">
		<li>Invalid data set XML source. Cannot display result.</li>
	</xsl:template>

	<xsl:template match="/dif:DIF">
		<li>
			<xsl:variable name="citation">
				<xsl:for-each select="dif:Data_Set_Citation">
					<big><b><xsl:for-each select="dif:Dataset_Creator[normalize-space(.)!='']">
						<xsl:value-of select="normalize-space(.)"/>
						<xsl:if test="position()!=last()">; </xsl:if>
					</xsl:for-each>
					<xsl:if test="normalize-space(dif:Dataset_Release_Date)!=''"><xsl:text> (</xsl:text><xsl:value-of select="date:year(normalize-space(dif:Dataset_Release_Date))" /><xsl:text>)</xsl:text></xsl:if>
					<xsl:text>: </xsl:text></b>
					<xsl:for-each select="dif:Dataset_Title[normalize-space(.)!='']|../dif:Entry_Title[normalize-space(.)!='']"><xsl:if test="position()=1"><xsl:value-of select="normalize-space(.)" /></xsl:if></xsl:for-each>
					</big>
				</xsl:for-each>
			</xsl:variable>
			<div><xsl:choose>
				<xsl:when test="normalize-space($url)!=''"><a href="{$url}" target="{$target}"><xsl:copy-of select="$citation"/></a></xsl:when>
				<xsl:otherwise><xsl:copy-of select="$citation"/></xsl:otherwise>
			</xsl:choose></div>
			<table border="0" cellspacing="0">
			<xsl:for-each select="dif:Data_Center[normalize-space(.)!='']">
				<tr><td valign="top" nowrap="nowrap"><em>Publisher/Source:</em>&#160;</td><td valign="top">
					<xsl:choose>
						<xsl:when test="normalize-space(dif:Data_Center_Name/dif:Short_Name)!=''">
							<xsl:value-of select="normalize-space(dif:Data_Center_Name/dif:Short_Name)"/>
							<xsl:if test="normalize-space(dif:Data_Center_Name/dif:Long_Name)!=''"><xsl:text> (</xsl:text><xsl:value-of select="normalize-space(dif:Data_Center_Name/dif:Long_Name)"/><xsl:text>)</xsl:text></xsl:if>
						</xsl:when>
						<xsl:otherwise>
							<xsl:value-of select="dif:Data_Center_Name/dif:Long_Name"/>
						</xsl:otherwise>
					</xsl:choose>
				</td></tr>
			</xsl:for-each>
			<xsl:for-each select="dif:Summary[normalize-space(.)!='' and (not(contains(.,'see the full metadata description')) or contains(.,' ** For all'))]">
				<tr><td valign="top" nowrap="nowrap"><em>Summary:</em>&#160;</td><td valign="top"><xsl:value-of select="normalize-space(.)"/></td></tr>
			</xsl:for-each>
			<xsl:if test="dif:Personnel[contains(dif:Role,'nvestigat')]"><tr><td valign="top" nowrap="nowrap"><em>Investigators:</em>&#160;</td><td valign="top">
				<xsl:for-each select="dif:Personnel[contains(string(dif:Role),'nvestigator')]">
					<xsl:value-of select="dif:Last_Name"/>
					<xsl:if test="normalize-space(dif:First_Name)!=''"><xsl:text>, </xsl:text><xsl:value-of select="dif:First_Name"/></xsl:if>
					<xsl:if test="position()!=last()">; </xsl:if>
				</xsl:for-each>
			</td></tr></xsl:if>
			<xsl:if test="dif:Parameters[normalize-space(dif:Variable|dif:Detailed_Variable)!='']"><tr><td valign="top" nowrap="nowrap"><em>Parameters:</em>&#160;</td><td valign="top">
				<xsl:for-each select="dif:Parameters[normalize-space(dif:Variable|dif:Detailed_Variable)!='']">
					<xsl:choose>
						<xsl:when test="normalize-space(dif:Detailed_Variable)!=''"><xsl:value-of select="normalize-space(dif:Detailed_Variable)"/></xsl:when>
						<xsl:otherwise><xsl:value-of select="normalize-space(dif:Variable)"/></xsl:otherwise>
					</xsl:choose>
					<xsl:if test="position()!=last()">; </xsl:if>
				</xsl:for-each>
			</td></tr></xsl:if>
			</table>
			<div style="margin-bottom:.8em"><small>
				<xsl:choose>
					<xsl:when test="normalize-space($url)!=''"><a href="{$url}" target="{$target}">Data Description</a></xsl:when>
					<xsl:otherwise>No URL to data set available</xsl:otherwise>
				</xsl:choose>
				<xsl:if test="$showinfo"><xsl:text> - Score: </xsl:text><xsl:comment>RELEVANCE START</xsl:comment><xsl:value-of select="$score" /><xsl:comment>RELEVANCE END</xsl:comment></xsl:if>
			</small></div>
		</li>
	</xsl:template>

</xsl:stylesheet>
