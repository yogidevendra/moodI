<?xml version="1.0" encoding="UTF-8"?>
<!--
  ~ Copyright (c) 2012-2017 DataTorrent, Inc.
  ~ All Rights Reserved.
  ~ The use of this source code is governed by the Limited License located at
  ~ https://www.datatorrent.com/datatorrent-openview-software-license/
  -->

<!--
    Document   : XmlJavadocCommentsExtractor.xsl
    Created on : September 16, 2014, 11:30 AM
    Description:
        The transformation strips off all information except for comments and tags from xml javadoc generated by xml-doclet.
-->

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
  <xsl:output method="xml" standalone="yes"/>

  <!-- copy xml by selecting only the following nodes, attributes and text -->
  <xsl:template match="node()|text()|@*">
    <xsl:copy>
      <xsl:apply-templates select="root|package|class|interface|method|field|type|comment|tag|text()|@name|@qualified|@text"/>
    </xsl:copy>
  </xsl:template>

  <!-- Strip off the following paths from the selected xml -->
  <xsl:template match="//root/package/interface/interface
                      |//root/package/interface/method/@qualified
                      |//root/package/class/interface
                      |//root/package/class/class
                      |//root/package/class/method/@qualified
                      |//root/package/class/field/@qualified" />

  <xsl:strip-space elements="*"/>
</xsl:stylesheet>
