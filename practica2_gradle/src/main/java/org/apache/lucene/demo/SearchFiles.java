package org.apache.lucene.demo;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.*;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Date;

import opennlp.tools.cmdline.postag.POSModelLoader;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSSample;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.WhitespaceTokenizer;
import opennlp.tools.util.ObjectStream;
import opennlp.tools.util.PlainTextByLineStream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;
import opennlp.tools.postag.POSTagger;
import opennlp.tools.namefind.DocumentNameFinder;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.sound.midi.SysexMessage;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

/** Simple command-line based search demo. */
public class SearchFiles {

  private SearchFiles() {
  }

  /**
   * Simple command-line based search demo.
   */
  public static void main(String[] args) throws Exception {
    String usage =
            "Usage:\tSearchFiles -index <indexPath> -query <queryFile> -output <resultsFile>;";
    if (args.length > 0 && ("-h".equals(args[0]) || "-help".equals(args[0]))) {
      System.out.println(usage);
      System.exit(0);
    }

    String index = "index";
    String output = null;
    String infoNeeds = null;

    String field = "contents";

    for (int i = 0; i < args.length; i++) {
      if ("-index".equals(args[i])) {
        index = args[i + 1];
        i++;
      } else if ("-infoNeeds".equals(args[i])) {
        infoNeeds = args[i + 1];
        i++;
      } else if ("-output".equals(args[i])) {
        output = args[i + 1];
        i++;
      }
    }

    IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(index)));
    IndexSearcher searcher = new IndexSearcher(reader);

    Similarity classic = new ClassicSimilarity();
    searcher.setSimilarity(classic);

    Analyzer analyzer = new SpanishAnalyzer2();

    //File extension is .xml
    if (infoNeeds.substring(infoNeeds.length() - 3, infoNeeds.length()).equals("xml")) {

      File file = new File(infoNeeds);
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

      DocumentBuilder db = dbf.newDocumentBuilder();
      org.w3c.dom.Document doc = db.parse(file);
      doc.getDocumentElement().normalize();
      NodeList infoList = doc.getElementsByTagName("informationNeed");

      POSModel model = new POSModelLoader().load(new File("opennlp-es-pos-maxent-pos-es.model"));
      POSTaggerME tagger = new POSTaggerME(model);

      for (int i = 0; i < infoList.getLength(); i++) {

        Node node = infoList.item(i);
        //System.out.println("\nNode Name :" + node.getNodeName());

        if (node.getNodeType() == Node.ELEMENT_NODE){
          Element e = (Element) node;
          //System.out.println("Identifier: "+ e.getElementsByTagName("identifier").item(0).getTextContent());
          //System.out.println("Text: "+ e.getElementsByTagName("text").item(0).getTextContent());

          String id =  e.getElementsByTagName("identifier").item(0).getTextContent();
          String text =  e.getElementsByTagName("text").item(0).getTextContent();

          WhitespaceTokenizer whitespaceTokenizer= WhitespaceTokenizer.INSTANCE;
          String[] tokens = whitespaceTokenizer.tokenize(text);
          String[] tags = tagger.tag(tokens);
          //System.out.println("Tokens: " + Arrays.toString(tokens));
          //System.out.println("Tagger: " + Arrays.toString(tags));

          POSSample tagged_text = new POSSample(tokens, tags);
          String query_text = tagged_text.toString();
          //System.out.println("Tagged text: " + query_text);

          String[] fields_N = {"titulo", "tipo", "descripcion", "autor", "departamento", "director"};
          String[] fields_ARV = {"titulo", "descripcion"};
          String[] fields_Z = {"titulo", "fecha", "descripcion"};

          BooleanClause.Occur[] flags_N = {BooleanClause.Occur.SHOULD , BooleanClause.Occur.SHOULD , BooleanClause.Occur.SHOULD,
                  BooleanClause.Occur.SHOULD , BooleanClause.Occur.SHOULD , BooleanClause.Occur.SHOULD};
          BooleanClause.Occur[] flags_ARV = {BooleanClause.Occur.SHOULD , BooleanClause.Occur.SHOULD};
          BooleanClause.Occur[] flags_Z = {BooleanClause.Occur.SHOULD , BooleanClause.Occur.SHOULD , BooleanClause.Occur.SHOULD};

          Query query = null, final_query = null;
          TopDocs results;
          ScoreDoc[] hits;
          int numTotalHits = 0;
          PrintWriter out = new PrintWriter(output, "UTF-8");

          for (String word : query_text.split("\\s+")) {
            String type = word.substring(word.indexOf("_") + 1, word.length());
            word = word.substring(0, word.indexOf("_")).replaceAll("[-+.^:,()*]","");;

            if (type.charAt(0) == 'N' ) {
              query = MultiFieldQueryParser.parse (word, fields_N , flags_N , analyzer );
              if (final_query == null) final_query = query;
              else final_query = new BooleanQuery.Builder()
                      .add(query ,BooleanClause.Occur.SHOULD )
                      .add(final_query, BooleanClause.Occur.SHOULD ).build();

            } else if (type.charAt(0) == 'Z' ) {
              query = MultiFieldQueryParser.parse (word, fields_Z , flags_Z , analyzer );
              if (final_query == null) final_query = query;
              else final_query = new BooleanQuery.Builder()
                      .add(query ,BooleanClause.Occur.SHOULD )
                      .add(final_query, BooleanClause.Occur.SHOULD ).build();

            } else if (type.charAt(0) == 'A' || type.charAt(0) == 'R' || type.charAt(0) == 'V') {
              query = MultiFieldQueryParser.parse(word, fields_ARV , flags_ARV , analyzer );
              if (final_query == null) final_query = query;
              else final_query = new BooleanQuery.Builder()
                      .add(query ,BooleanClause.Occur.SHOULD )
                      .add(final_query, BooleanClause.Occur.SHOULD ).build();
            }

          }

          System.out.println(final_query);
          results = searcher.search(final_query, 1000);
          hits = results.scoreDocs;
          numTotalHits = Math.toIntExact(results.totalHits.value);
          if (numTotalHits > 0) {
            hits = searcher.search(final_query, numTotalHits).scoreDocs;
            for (int j = 0; j < numTotalHits; j++) {
              Document search_doc = searcher.doc(hits[j].doc);
              String path = search_doc.get("path");
              if (path != null) {
                out.println(id + "  " + path);
              } else {
                out.println(id + "  " + "No path");
              }
            }
          }

        }
      }
    } else
    { //File extension is .txt

      BufferedReader in = null;
      in = new BufferedReader(new InputStreamReader(new FileInputStream(infoNeeds), "UTF-8"));
      PrintWriter out = new PrintWriter(output, "UTF-8");
      QueryParser parser = new QueryParser(field, analyzer);
      int queryNum = 0;

      while (true) {
        queryNum++;
        String line = in.readLine();

        if (line == null) {
          break;
        }

        line = line.trim();
        if (line.length() == 0) {
          break;
        }

        Query final_query;
        //line = spatial:<west>,<east>,<south>,<north>
        if (line.substring(0, 7).equals("spatial")) {

          String spatial, not_spatial = "";
          int ind = line.indexOf(" ");
          //Not only spatial
          if (ind != -1) {
            spatial = line.substring(0,ind);
            not_spatial = line.substring(ind+1, line.length());

          } else { //Only spatial
            spatial = line;
          }

          String str_values = spatial.substring(8,spatial.length());
          String[] values = str_values.split(",");
          double west = Double.parseDouble(values[0]);
          double south = Double.parseDouble(values[2]);
          double east = Double.parseDouble(values[1]);
          double north = Double.parseDouble(values[3]);

          // System.out.println(west + "," + south + "," + east + "," + north);

          //Xmin <= east
          Query westRangeQuery = DoublePoint.newRangeQuery ("west", Double.NEGATIVE_INFINITY ,east);
          //Xmax >= west
          Query eastRangeQuery = DoublePoint.newRangeQuery ("east", west ,Double.POSITIVE_INFINITY);
          //Ymax >= south
          Query northRangeQuery = DoublePoint.newRangeQuery ("north", south ,Double.POSITIVE_INFINITY);
          //Ymin <= north
          Query southRangeQuery = DoublePoint.newRangeQuery ("south", Double.NEGATIVE_INFINITY ,north);

          BooleanQuery spatial_query = new BooleanQuery.Builder()
                  .add ( westRangeQuery ,BooleanClause.Occur.MUST )
                  .add ( southRangeQuery ,BooleanClause.Occur.MUST )
                  .add ( eastRangeQuery ,BooleanClause.Occur.MUST )
                  .add ( northRangeQuery ,BooleanClause.Occur.MUST ).build();

          //Not only spatial query
          if (ind != -1) {
            Query query = parser.parse(not_spatial);
            final_query = new BooleanQuery.Builder()
                    .add(spatial_query ,BooleanClause.Occur.SHOULD )
                    .add(query, BooleanClause.Occur.SHOULD ).build();
          } else {
            final_query = spatial_query;
          }
        } else { //No espatial in query
          final_query = parser.parse(line);
        }

        TopDocs results = searcher.search(final_query, 1000);
        ScoreDoc[] hits = results.scoreDocs;
        int numTotalHits = Math.toIntExact(results.totalHits.value);
        //System.out.println(numTotalHits + " total matching documents");

        if (numTotalHits > 0) {
          hits = searcher.search(final_query, numTotalHits).scoreDocs;

          out.print(numTotalHits + "  ");

          for (int i = 0; i < numTotalHits; i++) {
            Document doc = searcher.doc(hits[i].doc);
            String path = doc.get("path");
            if (path != null) {
              //System.out.println(queryNum + "  " + path);
              out.print(path.substring(0, 2) + ",");
            } else {
              //System.out.println((queryNum) + "  " + "No path for this document");
              out.println((queryNum) + "  " + "No path for this document");
            }
          }
          out.println();
        }

      }
      reader.close();
      out.close();
    }
  }
}