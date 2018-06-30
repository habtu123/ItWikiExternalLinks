import java.io.File;
import java.util.ArrayList;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;

public class XMLReader {

	public static void main(String[] args) {
		String text = "";
		String[] externalLinksMain; 
		String[] externalLinks;
		try {
		File inputFile = new File("Files/test.xml");
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(inputFile);
		doc.getDocumentElement().normalize();
		System.out.println("The root element is:" + doc.getDocumentElement().getNodeName());
		
		NodeList nList = doc.getElementsByTagName("page"); 
		
		System.out.println("==================================");
		
		for(int i = 0; i<nList.getLength(); i++) {
			Node nNode = nList.item(i);
			
			if(nNode.getNodeType() == Node.ELEMENT_NODE) {
				
				 Element eElement = (Element) nNode;
				 
				 text = eElement.getElementsByTagName("text").item(0).getTextContent();
				 externalLinksMain = text.split("(?<=== Collegamenti esterni ==)");				 
				 
				 Pattern p = Pattern.compile("|");			
				 
				 externalLinks =  externalLinksMain[1].split("\n");
				
				Matcher m  = p.matcher(externalLinksMain[1]);	
				
				//System.out.println(externalLinksMain[1]);
				
				 MatchResult mr = m.toMatchResult();
				ArrayList<String> tempLink= new ArrayList<>();
				tempLink.add(externalLinks[1].split("|").toString());
				 System.out.println(tempLink.get(0));
				 
//				 if(m.find()) {
	
					
//					 for(int j = 0; j <  externalLinks.length; j++)
//					 {
//						 Pattern prl = Pattern.compile("(http:\\/\\/www\\.|https:\\/\\/www\\.|http:\\/\\/|https:\\/\\/)?[a-z0-9]+([\\-\\.]{1}[a-z0-9]+)*\\.[a-z]{2,5}(:[0-9]{1,5})?");
//						 Matcher ml = prl.matcher(externalLinks[j]);
//						 if(ml.find()) {
//							 MatchResult mlr = ml.toMatchResult();
//							System.out.println(mlr.group(0) +mlr.group(1)  +"\n");
//						 }						 
//					 }
//					
//				 }
//	            
			}// END OF IF
		}// END OF FOR LOOP
		} // END OF TRY
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}
}
