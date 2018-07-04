package it.wiki.hadoop.map;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class XMLReader {
	public static  void main(String[] args) {
	String text = "";
	String[] externalLinksMain; 
	String[] externalLinks;
	try {
		File inputFile = new File("Files/test.xml");
		 DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
         DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
         Document doc = dBuilder.parse(inputFile);
         doc.getDocumentElement().normalize();
         System.out.println("Root element :" + doc.getDocumentElement().getNodeName());
	
		
		doc.getDocumentElement().normalize();
		
		NodeList nList = doc.getElementsByTagName("page");
		//context.write(new Text("el_from \t Title \t External Link"), NullWritable.get());
		for (int temp = 0; temp < nList.getLength(); temp++) {

			Node nNode = nList.item(temp);
			
			if (nNode.getNodeType() == Node.ELEMENT_NODE) {
				System.out.println("=================");
				Element eElement = (Element) nNode;
				String revNode = eElement.getElementsByTagName("revision").item(0).getTextContent();
//
				String title = eElement.getElementsByTagName("title").item(0).getTextContent();
					text = eElement.getElementsByTagName("text").item(0).getTextContent();
				String id = eElement.getElementsByTagName("id").item(0).getTextContent(); 
				
				 externalLinksMain = text.split("(?<=== Collegamenti esterni ==)(.*)");	
				 externalLinks =  externalLinksMain[1].split("\n");
				 
				// System.out.println(externalLinksMain[1]);
				 for(int j = 0; j <  externalLinks.length; j++)
				 {
					 Pattern prl = Pattern.compile("(?:(?:https?|ftp):\\/\\/)?[\\w/\\-?=%.]+\\.[\\w/\\-?=%.]+");
					 //Pattern prl = Pattern.compile("(http|ftp|https)://([\\w_-]+(?:(?:\\.[\\w_-]+)+))([\\w.,@?^=%&:/~+#-]*[\\w@?^=%&/~+#-])?");
					 Matcher ml = prl.matcher(externalLinks[j]);
					 if(ml.find()) {
						 MatchResult mlr = ml.toMatchResult();
						System.out.println(mlr.group(0));
						// context.write(new Text(title),mynull.get());
					 }						 
				 }
			}
		}
	} catch (Exception e) {
		// LogWriter.getInstance().WriteLog(e.getMessage());
	}
	}
}
