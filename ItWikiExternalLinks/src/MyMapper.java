import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

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
					
					
					context.write(new Text(revision + "," + title), NullWritable.get());

				}
			}
		} catch (Exception e) {
			// LogWriter.getInstance().WriteLog(e.getMessage());
		}

	}

}