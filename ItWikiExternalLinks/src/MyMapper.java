import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	// private static final Log LOG = LogFactory.getLog(MyMapper.class);
	
	// Fprivate Text videoName = new Text();
	private String revision = "";
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String text = "";
		String[] externalLinksMain; 
		String[] externalLinks;
		try {

			InputStream is = new ByteArrayInputStream(value.toString().getBytes());
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(is);

			doc.getDocumentElement().normalize();

			NodeList nList = doc.getElementsByTagName("page");

			for (int temp = 0; temp < nList.getLength(); temp++) {

				Node nNode = nList.item(temp);
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {

					Element eElement = (Element) nNode;

					String revNode = eElement.getElementsByTagName("revision").item(0).getTextContent();

					String title = eElement.getElementsByTagName("title").item(0).getTextContent();
						text = eElement.getElementsByTagName("text").item(0).getTextContent();
					 externalLinksMain = text.split("(?<=== Collegamenti esterni ==)");	
					 externalLinks =  externalLinksMain[1].split("\n");
					 
					 for(int j = 0; j <  externalLinks.length; j++)
					 {
						 Pattern prl = Pattern.compile("(http:\\/\\/www\\.|https:\\/\\/www\\.|http:\\/\\/|https:\\/\\/)?[a-z0-9]+([\\-\\.]{1}[a-z0-9]+)*\\.[a-z]{2,5}(:[0-9]{1,5})?");
						 Matcher ml = prl.matcher(externalLinks[j]);
						 if(ml.find()) {
							 MatchResult mlr = ml.toMatchResult();
							 context.write(new Text(mlr.group(0) + "," + title), NullWritable.get());
						 }						 
					 }
					
					 
					

				}
			}
		} catch (Exception e) {
			// LogWriter.getInstance().WriteLog(e.getMessage());
		}

	}

}