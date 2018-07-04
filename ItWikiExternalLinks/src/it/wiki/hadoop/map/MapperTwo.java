package it.wiki.hadoop.map;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import it.wiki.hadoop.externallinks.*;

public class MapperTwo extends Mapper<LongWritable, Text, Text, IntWritable>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String text = "";
		String[] externalLinksMain; 
		String[] externalLinks;
		long idt  = 0 ;
		int increment = 0; 
		try {
			InputStream is = new ByteArrayInputStream(value.toString().getBytes());
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(is);
		
			
			doc.getDocumentElement().normalize();
			
			NodeList nList = doc.getElementsByTagName("page");
			//context.write(new Text("el_from \t Title \t External Link"), NullWritable.get());
			for (int temp = 0; temp < nList.getLength(); temp++) {

				Node nNode = nList.item(temp);
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {

					Element eElement = (Element) nNode;
					idt = context.getTaskAttemptID().getTaskID().getId();
					increment = context.getConfiguration().getInt("mapred.map.tasks", 0);
					String revNode = eElement.getElementsByTagName("revision").item(0).getTextContent();

					String title = eElement.getElementsByTagName("title").item(0).getTextContent();
						text = eElement.getElementsByTagName("text").item(0).getTextContent();
					String id = eElement.getElementsByTagName("id").item(0).getTextContent(); 
					
					 externalLinksMain = text.split("(?<=== Collegamenti esterni ==)");	
					 externalLinks =  externalLinksMain[1].split("\n");
					 
					 for(int j = 0; j <  externalLinks.length; j++)
					 {
						 Pattern prl = Pattern.compile("(?:(?:https?|ftp):\\/\\/)?[\\w/\\-?=%.]+\\.[\\w/\\-?=%.]+");
						 Matcher ml = prl.matcher(externalLinks[j]);
						 if(ml.find()) {
							 MatchResult mlr = ml.toMatchResult();
							 idt += increment;
							 context.write(new Text(id+","+title), new IntWritable(1));
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
