package org.lectures;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCRecord;
import org.archive.util.LaxHttpParser;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * read content from warc files and store into hbase
 */
public class WarcMapper extends Mapper<Text, Text, Text, Text> {

	protected Table table = null;
	private Logger logger = Logger.getLogger(WarcMapper.class);
	
	private static String TABLENAME = "webtable"; 
	private static String CFNAME = "cf"; 
	
	
	//TODO mimetype, text, date
	public enum fields {
		url, size
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);

		Configuration hBaseConfig = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(hBaseConfig);
		Admin admin = connection.getAdmin();
		HTableDescriptor tableDsc = new HTableDescriptor(
				TableName.valueOf(TABLENAME));
		if (!admin.tableExists(tableDsc.getTableName())) {
			tableDsc.addFamily(new HColumnDescriptor(CFNAME));
			admin.createTable(tableDsc);
			table = connection.getTable(tableDsc.getTableName());
		
			for (fields field : fields.values()) {
				createHBaseColumn(field.toString(), admin);
			}
		}
		table = connection.getTable(tableDsc.getTableName());
	}

	private void createHBaseColumn(String fieldName, Admin admin)
			throws IOException {
		HTableDescriptor tableDsc = table.getTableDescriptor();
		HColumnDescriptor colDsc = new HColumnDescriptor(fieldName);
		colDsc.setMaxVersions(HConstants.ALL_VERSIONS);
		admin.addColumn(tableDsc.getTableName(), colDsc);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// Closer.close(lilyClient);
		super.cleanup(context);
	}

	@Override
	protected void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {

		String path = key.toString();
		logger.info("path: " + path);

		try (FSDataInputStream fis = FileSystem.get(new java.net.URI(path),
				context.getConfiguration()).open(new Path(path))) {

			try (ArchiveReader reader = ArchiveReaderFactory.get(path, fis, true)) {
				for (ArchiveRecord archiveRecord : reader) {
					WARCRecord warc = (WARCRecord) archiveRecord;
					logger.info("warc header mimetype: " + warc.getHeader().getMimetype());
					if (warc.getHeader().getMimetype()
							.equals("application/http; msgtype=response")) {

						//url
						String url = warc.getHeader().getUrl();
						logger.info("url: " + url);

						Put put = new Put(Bytes.toBytes(url));
						put.addColumn(Bytes.toBytes(CFNAME),
								Bytes.toBytes(fields.url.toString()), Bytes.toBytes(url));

						//date
						//String dateString = warc.getHeader().getDate();


						// mimetype
						//String headerLine;
						//do {
						//	headerLine = LaxHttpParser.readLine(warc, "UTF-8");
						//	logger.debug("headerLine: " + headerLine);
						//} while (!headerLine.equals(""));


						//size
						long length = warc.getHeader().getLength() - warc.getPosition();
						int sizeLimit = Integer.MAX_VALUE - 1024;
						byte[] body = readBytes(warc, length, sizeLimit);
						logger.info("size: "+body.length);
						put.addColumn(Bytes.toBytes(CFNAME),
								Bytes.toBytes(fields.size.toString()),
								Bytes.toBytes(body.length));

						//body 
						//if mimetype is: text/html; charset=utf-8
						//if (contentType.startsWith("text/html") && contentType.contains("=")) {
						//String charset = contentType.substring(contentType.indexOf('=') + 1);
						//String domainName = url.split("/")[2];
						//String decodedBody = new String(body, charset);

						//write record to hbase
						table.put(put);
					}
				}
			}
		} catch (Exception e) {
			logger.error("map error: ", e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * Read the bytes of the record into a buffer and return the buffer. Give a
	 * size limit to the buffer to prevent from exploding memory, but still read
	 * all the bytes from the stream even if the buffer is full. This way, the
	 * file position will be advanced to the end of the record.
	 */
	private static byte[] readBytes(ArchiveRecord record, long contentLength,
			int sizeLimit) throws IOException {
		// Ensure the record does strict reading.
		record.setStrict(true);

		int actualSizeLimit = (int) Math.min(sizeLimit, contentLength);

		byte[] bytes = new byte[actualSizeLimit];

		if (actualSizeLimit == 0) {
			return bytes;
		}

		// NOTE: Do not use read(byte[]) because ArchiveRecord does NOT
		// over-ride
		// the implementation inherited from InputStream. And since it does
		// not over-ride it, it won't do the digesting on it. Must use either
		// read(byte[],offset,length) or read().
		int pos = 0;
		int c = 0;
		while (((c = record.read(bytes, pos, (bytes.length - pos))) != -1)
				&& pos < bytes.length) {
			pos += c;
		}

		// Now that the bytes[] buffer has been filled, read the remainder
		// of the record so that the digest is computed over the entire
		// content.
		byte[] buf = new byte[1024 * 1024];
		long count = 0;
		while (record.available() > 0) {
			count += record.read(buf, 0, Math.min(buf.length, record.available()));
		}

		// Sanity check. The number of bytes read into our bytes[]
		// buffer, plus the count of extra stuff read after it should
		// equal the contentLength passed into this function.
		if (pos + count != contentLength) {
			throw new IOException(
					"Incorrect number of bytes read from ArchiveRecord: expected="
							+ contentLength + " bytes.length=" + bytes.length + " pos=" + pos
							+ " count=" + count);
		}

		return bytes;
	}
}
