package org.kairosdb.plugin.kafka;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.exception.DatastoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 Created by bhawkins on 2/17/14.
 */
public class ConsumerThread implements Runnable
{
	public static final Logger logger = LoggerFactory.getLogger(ConsumerThread.class);

	private final Datastore m_datastore;
	private final String m_topic;
	private final KafkaStream<byte[], byte[]> m_stream;
	private final int m_threadNumber;
	private TopicParser m_topicParser;

	public ConsumerThread(Datastore datastore, String topic, KafkaStream<byte[], byte[]> stream, int threadNumber)
	{
		m_datastore = datastore;
		m_topic = topic;
		m_stream = stream;
		m_threadNumber = threadNumber;
	}

	public void setTopicParser(TopicParser parser)
	{
		m_topicParser = parser;
	}

	@Override
	public void run()
	{
		Thread.currentThread().setName(this.m_topic + "-" + this.m_threadNumber);
		logger.info("starting consumer thread " + this.m_topic + "-" + this.m_threadNumber);
		for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : m_stream)
		{
			try
			{
				logger.debug("message: " + messageAndMetadata.message());
				DataPointSet set = m_topicParser.parseTopic(m_topic, messageAndMetadata.key(),
						messageAndMetadata.message());

				for (DataPoint dataPoint : set.getDataPoints())
				{
					m_datastore.putDataPoint(set.getName(), set.getTags(), dataPoint);
				}

				//m_counter.incrementAndGet();
			}
			catch (DatastoreException e)
			{
				// TODO: rewind to previous message to provide consistent consumption
				logger.error("Failed to store datapoints: ", e);
			}
			catch (Exception e)
			{
				logger.error("Failed to parse message: " + messageAndMetadata.message(), e.getMessage());
			}
		}
	}
}
